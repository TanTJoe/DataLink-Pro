"""核心处理引擎 V50。"""
import os, re, gc, time, logging, datetime, traceback
from typing import List, Optional, Callable, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd, numpy as np, polars as pl
from core.config import XLSX_ENGINE
from core.models import MatchRule, OutputTask, PivotConfig
from core.parser import split_smart
from core.io_engine import read_smart_cached, read_and_combine_sheets, read_and_merge_folder

LogFunc = Optional[Callable[[str,str],None]]
ProgFunc = Optional[Callable[[float,str,str],None]]

_STOP_FLAG = False
_MAX_DATA_ROWS = 1048576 - 1

def request_stop():
    global _STOP_FLAG; _STOP_FLAG = True
def reset_stop():
    global _STOP_FLAG; _STOP_FLAG = False
def _check_stop():
    if _STOP_FLAG: raise InterruptedError("用户已手动停止任务")

def _is_oom(ex):
    if isinstance(ex, MemoryError): return True
    s = str(ex).lower()
    return any(k in s for k in ["out of memory","memoryerror","malloc","alloc","内存不足"])

def get_oom_advice():
    return "检测到内存不足。建议：1)用结构配置器减少列 2)转Parquet 3)拆分任务 4)升级内存"

class RunReport:
    def __init__(self): self.records=[]; self._t0=time.time()
    def add(self,stage,status,detail=""):
        try: self.records.append({"时间":datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"阶段":stage,"状态":status,"详情":str(detail)[:5000]})
        except: pass
    @property
    def elapsed(self): return time.time()-self._t0
    def flush_to_excel(self,out_path,extra_error=""):
        if not self.records: return
        try:
            rep=pd.DataFrame(self.records)
            if extra_error: rep.loc[len(rep)]=[datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"致命错误","FAIL",str(extra_error)]
            mode="a" if os.path.exists(out_path) else "w"
            if mode=="a":
                with pd.ExcelWriter(out_path,engine="openpyxl",mode="a",if_sheet_exists="replace") as w: rep.to_excel(w,sheet_name="程序执行报告",index=False)
            else:
                with pd.ExcelWriter(out_path,engine="openpyxl",mode="w") as w: rep.to_excel(w,sheet_name="程序执行报告",index=False)
        except Exception as e: logging.error(f"报告写入失败: {e}")

def execute_pipeline(m_path, out_path, rules, tasks, master_sheet, master_is_folder,
                     master_schema_cols, log_func=None, prog_func=None, folder_sheet=0):
    report=RunReport()
    def log(msg,level="info"):
        if log_func: log_func(msg,level)
    def prog(val,txt,mode='determinate'):
        if prog_func: prog_func(val,txt,mode)
    reset_stop()
    try:
        if os.path.exists(out_path):
            try:
                with open(out_path,'a'): pass
            except PermissionError:
                raise PermissionError(f"输出文件被占用：{out_path}")

        prog(5,"正在读取主表...",'indeterminate')
        log(">> 读取主表...")
        if os.path.isdir(m_path) or master_is_folder:
            pool_pl=read_and_merge_folder(m_path,required_cols=master_schema_cols,log_func=log_func,
                                          prog_func=lambda v,t: prog(v,t,'determinate'), folder_sheet=folder_sheet)
        else:
            usecols=master_schema_cols
            if isinstance(master_sheet,list) and len(master_sheet)>1:
                pool_pl=read_and_combine_sheets(m_path,master_sheet,usecols=usecols)
            else:
                sheet=master_sheet if master_sheet is not None else 0
                pool_pl=read_smart_cached(m_path,sheet=sheet,header_rows=0,usecols=usecols,log_func=log_func)
            if usecols: pool_pl=pool_pl.select([c for c in usecols if c in pool_pl.columns])
        report.add("主表读取","完成",f"行={pool_pl.height} 列={len(pool_pl.columns)}")
        _check_stop()

        if rules:
            log(f">> 处理{len(rules)}个匹配规则...")
            cache={}
            with ThreadPoolExecutor(max_workers=4) as exe:
                fm={exe.submit(read_smart_cached,r.target_path,r.sheet_idx,log_func=log_func):r for r in rules}
                for idx,f in enumerate(as_completed(fm)):
                    rule=fm[f]; prog(10+(idx/len(rules))*20,f"读取:{os.path.basename(rule.target_path)}",'determinate')
                    try: cache[id(rule)]=f.result()
                    except Exception as e: log(f"读取失败 {os.path.basename(rule.target_path)}: {e}","error")
            for idx,r in enumerate(rules):
                if id(r) not in cache: continue
                try:
                    prog(30+(idx/len(rules))*20,f"合并:{os.path.basename(r.target_path)}",'determinate')
                    tpl=cache[id(r)]; r.parse(pool_pl.columns,tpl.columns)
                    rmap={c:f"{r.col_prefix}{c}" for c in r._p_cols}
                    sub=tpl.select(r._t_cols+r._p_cols).unique(subset=r._t_cols)
                    sub=sub.rename({o:rmap.get(o,o) for o in sub.columns})
                    pool_pl=pool_pl.with_columns([pl.col(c).cast(pl.Utf8) for c in r._m_cols if c in pool_pl.columns])
                    sub=sub.with_columns([pl.col(c).cast(pl.Utf8) for c in r._t_cols if c in sub.columns])
                    pool_pl=pool_pl.join(sub,left_on=r._m_cols,right_on=r._t_cols,how="left",suffix="_dup")
                    log(f">> 已合并: {os.path.basename(r.target_path)}")
                    report.add(f"匹配:{os.path.basename(r.target_path)}","完成",f"取回{len(r._p_cols)}列")
                except Exception as e:
                    log(f"合并失败: {e}","error"); report.add(f"匹配:{os.path.basename(r.target_path)}","FAIL",str(e))

        _check_stop(); prog(60,"转换引擎...",'indeterminate')
        try:
            pool_df=pool_pl.to_pandas(); report.add("引擎转换","完成",f"行={len(pool_df)}")
        except Exception as e:
            report.flush_to_excel(out_path,str(e))
            if _is_oom(e): raise MemoryError(get_oom_advice()) from e
            raise

        prog(65,"优化数据类型...",'determinate')
        for col in pool_df.columns:
            try: pool_df[col]=pd.to_numeric(pool_df[col],errors='ignore')
            except: pass
        for col in pool_df.columns:
            if pool_df[col].dtype=='object':
                sample=pool_df[col].dropna().head(10).astype(str)
                if len(sample)>0 and sum(sample.str.match(r'^\d{4}[-/]\d{1,2}[-/]\d{1,2}'))>=len(sample)/2:
                    try: pool_df[col]=pd.to_datetime(pool_df[col],errors='coerce')
                    except: pass

        log(f">> 执行{len(tasks)}个输出任务...")
        _check_stop()
        with pd.ExcelWriter(out_path,engine=XLSX_ENGINE,datetime_format='yyyy-mm-dd',date_format='yyyy-mm-dd') as writer:
            for i,t in enumerate(tasks):
                report.add(f"任务:{t.name}","开始")
                try:
                    prog(70+((i+1)/len(tasks)*30),f"任务[{i+1}/{len(tasks)}]: {t.name}",'determinate')
                    _check_stop()
                    res=_execute_single_task(t,pool_df,log)
                    _write_task_to_excel(writer,t.name,res,log)
                    report.add(f"任务:{t.name}","完成",f"行={len(res)} 列={len(res.columns)}")
                    gc.collect()
                except Exception as e:
                    report.add(f"任务:{t.name}","FAIL",str(e))
                    log(f"任务失败 {t.name}: {e}","error")
                    if _is_oom(e): raise
                    continue
        prog(100,"处理完毕",'determinate'); log(f"★ 全部完成! 耗时{report.elapsed:.1f}秒","success")
        report.add("任务流","完成",f"耗时={report.elapsed:.1f}s"); report.flush_to_excel(out_path)
    except InterruptedError:
        report.add("用户停止","STOP"); report.flush_to_excel(out_path); raise
    except PermissionError: raise
    except MemoryError: raise
    except Exception as e:
        traceback.print_exc(); report.add("致命错误","FAIL",str(e)); report.flush_to_excel(out_path,str(e)); raise

def _execute_single_task(task,pool_df,log):
    df=pool_df.copy(deep=False)
    if task.calc_fields:
        for _expr in split_smart(task.calc_fields):
            if '=' not in _expr: continue
            _lhs=_expr.split('=',1)[0].strip(); _m=re.match(r"df\[['\"](.+?)['\"]\]",_lhs)
            _col=_m.group(1) if _m else _lhs
            if _col in df.columns:
                try: df[_col]=df[_col].copy()
                except: pass
        for p in split_smart(task.calc_fields):
            p=p.strip()
            if not p: continue
            try:
                if '=' in p:
                    lhs,rhs=p.split('=',1); lhs=lhs.strip()
                    cmd=f"{lhs} = {rhs.strip()}" if lhs.startswith('df[') or lhs.startswith('df.') else f"df['{lhs}'] = {rhs.strip()}"
                else:
                    cmd=f"{p} = ''" if p.startswith('df[') else f"df['{p}'] = [None]*len(df)"
                exec(cmd,{'df':df,'pd':pd,'np':np})
            except Exception as e: log(f"  计算失败'{p}': {e}","error")
    if task.filter_expr:
        try: df=df.query(task.filter_expr)
        except Exception as e: log(f"  筛选忽略: {e}","error")
    res=_execute_pivot(task.pivot_cfg,df,log) if task.pivot_cfg.enabled else _execute_detail(task,df)
    if task.sort_config and not res.empty:
        by=[x['col'] for x in task.sort_config if x['col'] in res.columns]
        asc=[x['asc'] for x in task.sort_config if x['col'] in res.columns]
        if by:
            try: res.sort_values(by=by,ascending=asc,inplace=True,key=lambda c: c.str.lower() if c.dtype=='object' else c)
            except Exception as e: log(f"  排序失败: {e}","error")
    return res

def _execute_detail(task,df):
    src=[x['src'] for x in task.col_map]; ren={x['src']:x['dst'] for x in task.col_map}
    for c in src:
        if c not in df.columns: df[c]=""
    return df[src].rename(columns=ren)

def _execute_pivot(pc,df,log):
    agg={k.strip():v.strip() for k,v in pc.values_agg.items()}
    if not pc.group_by and not agg: raise ValueError("透视表需要分组和值")
    should_fallback=True
    try:
        df_pl=pl.from_pandas(df)
        for c in pc.group_by+pc.pivot_cols+list(agg.keys()):
            if c in df_pl.columns and df_pl[c].dtype==pl.Object: df_pl=df_pl.with_columns(pl.col(c).cast(pl.Utf8))
        pl_aggs=[getattr(pl,fn) for fn in agg.values()]
        res_pl=df_pl.pivot(index=pc.group_by,columns=pc.pivot_cols,values=list(agg.keys()),aggregate_function=pl_aggs)
        res=res_pl.to_pandas(); should_fallback=False
    except Exception as e:
        logging.warning(f"Polars Pivot Failed: {e}")
    if should_fallback:
        res=pd.pivot_table(df,index=pc.group_by,columns=pc.pivot_cols or None,values=list(agg.keys()),aggfunc=agg).reset_index()
    if pc.pivot_cols:
        if isinstance(res.columns,pd.MultiIndex):
            res.columns=['_'.join(map(str,c)).strip('_') if isinstance(c,tuple) else c for c in res.columns.values]
        res.columns=[str(c) for c in res.columns]

    # V50: 透视表聚合值空值自动填0
    val_cols=[c for c in res.columns if c not in pc.group_by]
    for vc in val_cols:
        if res[vc].dtype in ('float64','float32','int64','int32'):
            null_count=res[vc].isna().sum()
            if null_count>0:
                res[vc]=res[vc].fillna(0)
                if log: log(f"  透视表列'{vc}'有{null_count}个空值已填0（该分组无数据）","info")

    # V50: 透视表重命名 - 批量前缀替换
    if pc.renames:
        rmap={}
        for pair in split_smart(pc.renames):
            if '=' not in pair: continue
            old,new=pair.split('=',1); old_key=old.strip(); new_val=new.strip()
            if old_key in res.columns:
                rmap[old_key]=new_val; continue
            matched=False
            for col in res.columns:
                cs=str(col)
                if cs.startswith(old_key+"_"):
                    rmap[cs]=new_val+cs[len(old_key):]; matched=True
                elif cs.endswith("_"+old_key):
                    rmap[cs]=cs[:len(cs)-len(old_key)]+new_val; matched=True
            if not matched:
                for col in res.columns:
                    stripped=re.sub(r'_(sum|avg|count|min|max|median|nunique|mean)$','',str(col))
                    if stripped==old_key: rmap[col]=new_val; break
        if rmap: res.rename(columns=rmap,inplace=True)
    return res

def _write_task_to_excel(writer,task_name,res,log):
    safe=re.sub(r'[^\w\u4e00-\u9fa5]+','_',task_name).strip('_')
    if not safe: safe='TASK'
    safe=safe[:31]
    wb=writer.book; hfmt=wb.add_format({'bold':True,'fg_color':'#084584','font_color':'white','valign':'vcenter'})
    if len(res)<=_MAX_DATA_ROWS:
        parts=[(safe,res)]
    else:
        n=int((len(res)+_MAX_DATA_ROWS-1)//_MAX_DATA_ROWS); base=safe[:28]
        parts=[(base if pi==0 else f"{base}_{pi+1}")[:31] for pi in range(n)]
        parts=[(sn,res.iloc[pi*_MAX_DATA_ROWS:(pi+1)*_MAX_DATA_ROWS]) for pi,(sn,_) in enumerate(zip(parts,[None]*n))]
        # fix: rebuild properly
        parts=[]
        for pi in range(n):
            sn=(base if pi==0 else f"{base}_{pi+1}")[:31]
            parts.append((sn,res.iloc[pi*_MAX_DATA_ROWS:(pi+1)*_MAX_DATA_ROWS]))
        log(f"  超大结果{task_name}: {len(res)}行→{len(parts)}个Sheet","info")
    for sn,pdf in parts:
        pdf.to_excel(writer,sheet_name=sn,index=False,startrow=1,header=False)
        ws=writer.sheets[sn]
        for ci,v in enumerate(pdf.columns.values):
            ws.write(0,ci,v,hfmt); ws.set_column(ci,ci,15)
