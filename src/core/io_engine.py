"""文件读写引擎 V50：Parquet原生 + Calamine极速 + 文件夹指定Sheet。"""
import os, glob, logging
from typing import List, Optional, Union, Callable
import pandas as pd
import polars as pl
from core.config import ENABLE_CACHE
from core.cache import make_cache_key, get_cache_path, try_read_cache, write_cache, make_master_merge_cache_key

LogFunc = Optional[Callable[[str,str],None]]
ProgFunc = Optional[Callable[[float,str],None]]

def detect_encoding(file_path):
    try:
        import chardet
    except ImportError: return 'utf-8'
    try:
        with open(file_path,'rb') as f: raw=f.read(20000)
        enc=(chardet.detect(raw) or {}).get('encoding')
        if enc and 'GB' in enc.upper(): return 'gbk'
        return enc or 'utf-8'
    except: return 'utf-8'

def get_sheet_names(path):
    try:
        if path.lower().endswith('.parquet'): return ['Parquet']
        if path.lower().endswith(('.xlsx','.xls')): return pd.ExcelFile(path).sheet_names
        return ['CSV']
    except: return []

def get_columns_preview(path, sheet=0):
    try:
        if path.lower().endswith('.parquet'):
            return list(pl.read_parquet_schema(path).keys())
        if path.lower().endswith('.csv'):
            df=pd.read_csv(path,nrows=1,encoding=detect_encoding(path))
            return [f'Unnamed_{i}' if 'Unnamed' in str(c) else str(c) for i,c in enumerate(df.columns)]
        if isinstance(sheet,int) and sheet>=0 and path.lower().endswith(('.xlsx','.xls')):
            sn=get_sheet_names(path)
            if not sn: return ["无法获取Sheet"]
            sheet=sn[min(sheet,len(sn)-1)]
        df=pd.read_excel(path,sheet_name=sheet,nrows=1,dtype=str)
        return [f'Unnamed_{i}' if 'Unnamed' in str(c) else str(c) for i,c in enumerate(df.columns)]
    except Exception as e:
        return [f"读取错误: {e}"]

def read_physical_polars(path, sheet=0, header_rows=0, usecols=None):
    """V50：Parquet原生 + CSV流式 + Calamine极速Excel。"""
    try:
        if path.lower().endswith('.parquet'):
            return pl.read_parquet(path, columns=usecols) if usecols else pl.read_parquet(path)
        if path.lower().endswith('.csv'):
            enc=detect_encoding(path)
            try:
                lf=pl.scan_csv(path,encoding=enc,ignore_errors=True,infer_schema_length=0,truncate_ragged_lines=True)
                df=lf.collect()
                if usecols: df=df.select([c for c in usecols if c in df.columns])
                return df
            except:
                try:
                    return pl.read_csv(path,encoding=enc,skip_rows=header_rows,ignore_errors=True,infer_schema_length=0,columns=usecols or None)
                except:
                    pdf=pd.read_csv(path,encoding='gbk',header=header_rows,on_bad_lines='skip',usecols=usecols or None)
                    pldf=pl.from_pandas(pdf)
                    if usecols: pldf=pldf.select([c for c in usecols if c in pldf.columns])
                    return pldf.cast(pl.String)
        elif path.lower().endswith(('.xlsx','.xls')):
            sheet_name=sheet
            if isinstance(sheet,int):
                sn=get_sheet_names(path)
                if sheet>=len(sn): raise ValueError(f"Sheet索引{sheet}超出范围")
                sheet_name=sn[sheet]
            for engine in ['calamine','xlsx2csv','openpyxl']:
                try:
                    df=pl.read_excel(path,sheet_name=sheet_name,infer_schema_length=0,engine=engine)
                    if usecols: df=df.select([c for c in usecols if c in df.columns])
                    return df
                except: continue
            logging.warning(f"Polars failed for {path}, falling back to Pandas")
            eng='openpyxl' if path.lower().endswith('.xlsx') else None
            pdf=pd.read_excel(path,sheet_name=sheet_name,header=header_rows,engine=eng,dtype=str,usecols=usecols or None)
            pdf.columns=[f'Unnamed_{i}' if 'Unnamed' in str(c) else c for i,c in enumerate(pdf.columns)]
            pldf=pl.from_pandas(pdf)
            if usecols: pldf=pldf.select([c for c in usecols if c in pldf.columns])
            return pldf
    except Exception as e:
        raise ValueError(f"读取失败: {e}")
    raise ValueError("不支持的文件格式")

def read_smart_cached(path, sheet=0, header_rows=0, usecols=None, log_func=None, force_refresh=False):
    path=str(path)
    if not os.path.exists(path): raise FileNotFoundError(path)
    if not ENABLE_CACHE or force_refresh:
        return read_physical_polars(path,sheet,header_rows,usecols)
    abs_path=os.path.abspath(path)
    ck=make_cache_key(abs_path,sheet,header_rows,usecols)
    cp=get_cache_path(ck)
    cached=try_read_cache(cp,log_func)
    if cached is not None: return cached
    df=read_physical_polars(abs_path,sheet,header_rows,usecols)
    write_cache(df,cp,log_func)
    return df

def read_and_combine_sheets(path, sheets, usecols=None):
    asn=get_sheet_names(path); final=[]
    for s in sheets:
        if isinstance(s,int) and 0<=s<len(asn): final.append(asn[s])
        elif isinstance(s,str) and s in asn: final.append(s)
        else: raise ValueError(f"无效Sheet: {s}")
    if not final: raise ValueError("未选择任何工作表")
    dfs=[read_smart_cached(path,sheet=sn,header_rows=0,usecols=usecols) for sn in final]
    return pl.concat(dfs,how="align")

def scan_data_files(folder):
    folder=str(folder)
    if not os.path.isdir(folder): return []
    files=[]
    for ext in ("*.xlsx","*.xls","*.csv","*.parquet"):
        files.extend(glob.glob(os.path.join(folder,ext)))
    return sorted([f for f in files if os.path.isfile(f) and not os.path.basename(f).startswith("~$")],
                  key=lambda x: os.path.getmtime(x))

def pick_template_file(files):
    return files[-1] if files else None

def read_and_merge_folder(folder, required_cols=None, log_func=None, prog_func=None, folder_sheet=0):
    """V50: 支持 folder_sheet 参数指定文件夹模式下读取的Sheet（索引或名称）。"""
    files=scan_data_files(folder)
    if not files: raise ValueError(f"文件夹内无可用文件: {folder}")
    template=pick_template_file(files)
    if log_func: log_func(f"文件夹模式：{len(files)}个文件，模板：{os.path.basename(template)}","info")
    if not required_cols:
        required_cols=get_columns_preview(template,sheet=folder_sheet)
        if log_func: log_func(f"自动使用模板列：{len(required_cols)}列","info")
    required_set=set(required_cols)
    cache_path=make_master_merge_cache_key(folder,files,required_cols)
    if cache_path and os.path.exists(cache_path):
        try:
            if log_func: log_func(f"[整表缓存命中]","info")
            if prog_func: prog_func(20,'读取整表缓存...')
            return pl.read_parquet(cache_path)
        except: pass
    ok_dfs=[]; skipped=0; total=len(files)
    for i,fp in enumerate(files,1):
        if prog_func: prog_func(5+(i/max(total,1))*15,f"合并[{i}/{total}]: {os.path.basename(fp)}")
        cols=get_columns_preview(fp,sheet=folder_sheet)
        if cols and isinstance(cols[0],str) and cols[0].startswith("读取错误:"):
            skipped+=1; continue
        if not required_set.issubset(set(cols)):
            skipped+=1
            if log_func: miss=[c for c in required_cols if c not in cols]; log_func(f"[跳过] {os.path.basename(fp)}：缺{miss[:5]}","warn")
            continue
        try:
            df=read_smart_cached(fp,sheet=folder_sheet,header_rows=0,usecols=required_cols,log_func=log_func)
            df=df.select([c for c in required_cols if c in df.columns])
            ok_dfs.append(df)
        except Exception as e:
            skipped+=1
            if log_func: log_func(f"[跳过] {os.path.basename(fp)}：{e}","warn")
    if not ok_dfs: raise ValueError("所有文件都失败了")
    if prog_func: prog_func(25,"Polars concat...")
    final=pl.concat(ok_dfs,how="vertical")
    if cache_path:
        try: final.write_parquet(cache_path)
        except: pass
    if log_func: log_func(f"合并完成：有效{len(ok_dfs)}个，跳过{skipped}个，{final.height}行","success")
    return final
