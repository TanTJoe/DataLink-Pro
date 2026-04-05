"""V50 主界面框架。"""
import os,re,json,time,queue,subprocess,threading,csv as csv_mod
import tkinter as tk
from tkinter import ttk,scrolledtext,messagebox,filedialog,simpledialog
from typing import List,Dict,Optional,Union
import polars as pl, pandas as pd
from ui.theme import THEME
from ui.components import ColumnViewer,SheetMultiPickerDialog,MasterSchemaDesignerDialog,VirtualColumnMapDialog
from ui.dialogs import RuleDialog,TaskEditDialog
from core.config import AUTOSAVE_FILE,APP_TITLE
from core.models import MatchRule,OutputTask,rule_to_dict,task_to_dict
from core.parser import idx_to_letter,resolve_cols_expr
from core.io_engine import get_sheet_names,get_columns_preview,scan_data_files,pick_template_file,read_physical_polars,detect_encoding
from core.processor import execute_pipeline,get_oom_advice,_is_oom
from utils.helpers import load_presets,save_presets,save_last_active,load_last_active,unique_path

class AdvancedReportFrame(ttk.Frame):
    PRESET_KEY="AdvancedFactory"
    def __init__(self,master):
        super().__init__(master,padding=15)
        self.rules=[]; self.tasks=[]; self.master_sheet=0; self.master_is_folder=False
        self.master_schema_cols=None; self.master_template_file=None; self.master_virtual_map={}
        self.master_schema_store={"file":{"last_cols":None,"last_path":""},"folder":{"last_cols":None,"last_path":""}}
        self.master_schema_by_source={}; self._master_schema_key=""; self.pool_columns=[]
        self.msg_queue=queue.Queue(); self.folder_sheet=0
        self.master.protocol("WM_DELETE_WINDOW",self._on_close)
        self._ui(); self._load_autosave(); self._load_last_active(); self._poll_queue()
        # V50: 每60秒自动保存一次状态，防止断电丢失
        self._auto_save_loop()

    def _ui(self):
        paned=ttk.PanedWindow(self,orient='horizontal'); paned.pack(fill='both',expand=True,pady=(0,10))
        fl=ttk.Frame(paned,padding=(0,0,10,0)); paned.add(fl,weight=1)
        ttk.Label(fl,text="Step 1: 数据池配置",font=THEME['font_large'],foreground=THEME['accent']).pack(fill='x',pady=5)
        fm=ttk.LabelFrame(fl,text="选择输入主表",padding=10); fm.pack(fill='x',pady=5)
        self.e_master=ttk.Entry(fm); self.e_master.pack(side='left',fill='x',expand=True)
        fb=ttk.Frame(fm); fb.pack(fill='x',pady=(5,0))
        for t,c in [("文件",lambda:self._pick(self.e_master,'file')),("Sheet",self._pick_master_sheet),
                     ("列名",self._view_master_cols),("文件夹",lambda:self._pick(self.e_master,'folder')),
                     ("结构",self._open_schema_designer),("多缓存",self._batch_convert_parquet),
                     ("单缓存",self._single_file_cache)]:
            ttk.Button(fb,text=t,command=c, width=6).pack(side='left',padx=2,pady=1)

        fr=ttk.LabelFrame(fl,text="目标匹配列表",padding=10); fr.pack(fill='both',expand=True,pady=5)
        self.lst_rules=tk.Listbox(fr,height=10,relief="flat",bg=THEME['input_bg'],selectbackground=THEME['accent'],selectforeground='white')
        self.lst_rules.pack(fill='both',expand=True); self.lst_rules.bind("<Double-1>",self._edit_rule)
        fbb=ttk.Frame(fr); fbb.pack(fill='x',pady=5)
        ttk.Button(fbb,text="+ 添加",width=10,command=self._add_rule).pack(side='left',padx=1)
        ttk.Button(fbb,text="- 删除",width=10,command=self._del_rule).pack(side='left',padx=1)
        ttk.Frame(fbb).pack(side='left',fill='x',expand=True)
        ttk.Button(fbb,text="▼",width=4,command=lambda:self._move_rule(1)).pack(side='right',padx=1)
        ttk.Button(fbb,text="▲",width=4,command=lambda:self._move_rule(-1)).pack(side='right',padx=1)
        ttk.Button(fl,text="加载数据池",command=self._analyze_pool,style="Accent.TButton").pack(fill='x',pady=10)

        fright=ttk.Frame(paned,padding=(10,0,0,0)); paned.add(fright,weight=1)
        ttk.Label(fright,text="Step 2: 输出任务设置",font=THEME['font_large'],foreground=THEME['accent']).pack(fill='x',pady=5)
        fo=ttk.LabelFrame(fright,text="结果输出路径",padding=10); fo.pack(fill='x',pady=5)
        self.e_out=ttk.Entry(fo); self.e_out.pack(side='left',fill='x',expand=True)
        ttk.Button(fo,text="选择",width=5,command=lambda:self._pick(self.e_out,'save')).pack(side='left')
        ft=ttk.LabelFrame(fright,text="输出任务列表",padding=10); ft.pack(fill='both',expand=True,pady=5)
        self.lst_tasks=tk.Listbox(ft,relief="flat",bg=THEME['input_bg'],selectbackground=THEME['accent'],selectforeground='white')
        self.lst_tasks.pack(fill='both',expand=True); self.lst_tasks.bind("<Double-1>",self._edit_task)
        tb=ttk.Frame(ft); tb.pack(fill='x',pady=5)
        ttk.Button(tb,text="+ 新增",width=10,command=self._add_task).pack(side='left',padx=1)
        ttk.Button(tb,text="编辑",width=10,command=self._edit_task).pack(side='left',padx=1)
        ttk.Button(tb,text="- 删除",width=10,command=self._del_task).pack(side='left',padx=1)
        ttk.Frame(tb).pack(side='left',fill='x',expand=True)
        ttk.Button(tb,text="▼",width=4,command=lambda:self._move_task(1)).pack(side='right',padx=1)
        ttk.Button(tb,text="▲",width=4,command=lambda:self._move_task(-1)).pack(side='right',padx=1)
        self._ui_footer()
        fa=ttk.Frame(self); fa.pack(fill='x',pady=5)
        self.btn_start=ttk.Button(fa,text="启动任务",command=self._start,style="Success.TButton")
        self.btn_start.pack(side='left',fill='x',expand=True,ipady=8)
        self.btn_stop=ttk.Button(fa,text="停止",command=self._stop_task,state='disabled')
        self.btn_stop.pack(side='right',padx=(10,0),ipady=8)

    def _ui_footer(self):
        f=ttk.Frame(self); f.pack(side='bottom',fill='x',pady=5)
        fp=ttk.Frame(f); fp.pack(fill='x')
        ttk.Label(fp,text="Step 3: 方案管理",font=THEME['font_large'],foreground=THEME['accent']).pack(side='left')
        self.cmb_presets=ttk.Combobox(fp,state='readonly',width=50); self.cmb_presets.pack(side='left',padx=5)
        self.cmb_presets.bind('<<ComboboxSelected>>',self._apply_preset)
        self.cmb_presets.bind('<MouseWheel>',lambda e:"break")
        for t,c in [("保存",self._save_preset),("删除",self._del_preset),("导出",self._export_preset),("导入",self._import_preset)]:
            ttk.Button(fp,text=t,width=5,command=c).pack(side='left',padx=2)
        ttk.Button(fp,text="新建项目",command=self._reset_project).pack(side='right',padx=5)
        self._refresh_presets()
        fpg=ttk.Frame(f); fpg.pack(fill='x',pady=5)
        self.var_status=tk.StringVar(value="准备就绪")
        ttk.Label(fpg,textvariable=self.var_status,font=("Segoe UI",8)).pack(anchor='w')
        self.prog=ttk.Progressbar(fpg,mode='determinate',style="Custom.Horizontal.TProgressbar"); self.prog.pack(fill='x')
        flh=ttk.Frame(f); flh.pack(fill='x')
        ttk.Label(flh,text="运行日志",font=THEME['font_bold']).pack(side='left')
        ttk.Button(flh,text="清空",width=5,command=self._clear_log).pack(side='right')
        self.log_txt=scrolledtext.ScrolledText(f,height=12,state='disabled',font=THEME['font_mono'],bg=THEME['panel'])
        self.log_txt.pack(fill='x')

    def _poll_queue(self):
        while not self.msg_queue.empty():
            try:
                kind,data=self.msg_queue.get_nowait()
                if kind=='log':
                    msg,level=data; self.log_txt.config(state='normal')
                    tag=level if level in ('error','success') else 'info'
                    self.log_txt.tag_config('error',foreground=THEME['error']); self.log_txt.tag_config('success',foreground=THEME['success'])
                    self.log_txt.insert(tk.END,f"[{time.strftime('%H:%M:%S')}] {msg}\n",tag); self.log_txt.see(tk.END); self.log_txt.config(state='disabled')
                elif kind=='prog':
                    val,txt,mode=data; self.var_status.set(txt)
                    if mode=='indeterminate':
                        if str(self.prog['mode'])!='indeterminate': self.prog.configure(mode='indeterminate',style="Indeterminate.Horizontal.TProgressbar"); self.prog.start(10)
                    else:
                        if str(self.prog['mode'])!='determinate': self.prog.stop(); self.prog.configure(mode='determinate',style="Custom.Horizontal.TProgressbar")
                        self.prog['value']=val
                elif kind=='finish':
                    op=self.e_out.get()
                    if op and os.path.exists(op):
                        try:
                            if os.name=='nt': subprocess.run(f'explorer /select,"{os.path.normpath(op)}"',shell=True)
                        except: pass
                    self.var_status.set("处理完毕"); self.prog.stop(); self.prog.configure(mode='determinate',style="Custom.Horizontal.TProgressbar"); self.prog['value']=100
                elif kind=='error_box':
                    messagebox.showerror("错误",data); self.var_status.set("发生错误"); self.prog.stop(); self.prog.configure(mode='determinate'); self.prog['value']=0
            except queue.Empty: break
        self.after(100,self._poll_queue)

    def _log_safe(self,msg,level="info"): self.msg_queue.put(('log',(msg,level)))
    def _prog_safe(self,val,txt="处理中...",mode='determinate'): self.msg_queue.put(('prog',(max(0,min(100,val)),txt,mode)))
    def _clear_log(self):
        self.log_txt.config(state='normal'); self.log_txt.delete('1.0',tk.END); self.log_txt.config(state='disabled')

    def _pick(self,entry,mode):
        if mode=='file': p=filedialog.askopenfilename(filetypes=[("Data","*.xlsx *.xls *.csv *.parquet")])
        elif mode=='folder': p=filedialog.askdirectory()
        else: p=filedialog.asksaveasfilename(defaultextension=".xlsx",filetypes=[("Excel","*.xlsx")])
        if not p: return
        entry.delete(0,tk.END); entry.insert(0,p)
        if entry==self.e_master:
            self._stash_current_master_schema()
            if os.path.isdir(p):
                self.master_is_folder=True; self.master_sheet=0; self.master_template_file=None
                self._log_safe(f"主表文件夹模式：{p}","info"); self._restore_master_schema_for(p,True)
            else:
                self.master_is_folder=False; self.master_template_file=None; self.master_schema_cols=None; self.master_virtual_map={}
                if p.lower().endswith(('.xlsx','.xls')):
                    ns=get_sheet_names(p); self.master_sheet=ns[0] if ns else 0
                else: self.master_sheet=0
                self._log_safe("主表已切换，Schema已重置","info")

    def _pick_master_sheet(self):
        mp=self.e_master.get().strip()
        if os.path.isdir(mp):
            # V50: 文件夹模式允许指定Sheet
            sheet_str=simpledialog.askstring("文件夹Sheet设置","输入Sheet名称或索引（默认0=第一个Sheet）:",initialvalue=str(self.folder_sheet))
            if sheet_str is not None:
                self.folder_sheet=int(sheet_str) if sheet_str.isdigit() else sheet_str
                self._log_safe(f"文件夹模式Sheet设为: {self.folder_sheet}","info")
            return
        if not os.path.exists(mp) or not mp.lower().endswith(('.xlsx','.xls')):
            messagebox.showwarning("提示","请先选择Excel文件"); return
        def cb(sheets):
            self.master_sheet=sheets
            self._log_safe(f"主表{'合并' if isinstance(sheets,list) else '单表'}模式：{sheets}","info")
        SheetMultiPickerDialog(self,mp,get_sheet_names(mp),cb)

    def _view_master_cols(self):
        mp=self.e_master.get().strip()
        if not os.path.exists(mp): messagebox.showwarning("提示","请先选择主表"); return
        if os.path.isdir(mp):
            if self.master_schema_cols: ColumnViewer(self,mp,0,cols=self.master_schema_cols); return
            files=scan_data_files(mp); template=pick_template_file(files)
            if not template: messagebox.showwarning("提示","无文件"); return
            def w():
                cols=get_columns_preview(template,self.folder_sheet)
                self.after(0,lambda:ColumnViewer(self,mp,self.folder_sheet,cols=cols))
            threading.Thread(target=w,daemon=True).start(); return
        if isinstance(self.master_sheet,list) and len(self.master_sheet)>1:
            messagebox.showinfo("提示","多Sheet合并模式，请先加载数据池"); return
        sheet=self.master_sheet or 0
        def w():
            cols=get_columns_preview(mp,sheet)
            self.after(0,lambda:ColumnViewer(self,mp,sheet,cols=cols))
        threading.Thread(target=w,daemon=True).start()

    # V50: 单文件空载转缓存
    def _single_file_cache(self):
        p=filedialog.askopenfilename(filetypes=[("Data","*.xlsx *.xls *.csv")],title="选择要预缓存的文件")
        if not p: return
        # 询问要转哪些sheet
        if p.lower().endswith(('.xlsx','.xls')):
            sheets=get_sheet_names(p)
            if len(sheets)>1:
                choice=messagebox.askyesnocancel("Sheet选择",f"该文件有{len(sheets)}个Sheet:\n{', '.join(sheets[:10])}\n\n是=全部转换 | 否=选择指定Sheet")
                if choice is None: return
                if choice: target_sheets=sheets
                else:
                    sel=simpledialog.askstring("指定Sheet","输入Sheet名称或索引(逗号分隔):",initialvalue="0")
                    if not sel: return
                    target_sheets=[]
                    for s in sel.split(','):
                        s=s.strip()
                        if s.isdigit() and int(s)<len(sheets): target_sheets.append(sheets[int(s)])
                        elif s in sheets: target_sheets.append(s)
                        else: self._log_safe(f"跳过无效Sheet: {s}","warn")
                    if not target_sheets: return
            else: target_sheets=sheets
        else: target_sheets=['CSV']
        self._prog_safe(0,"预缓存中...",'indeterminate')
        def worker():
            total=len(target_sheets); ok=0
            for i,sn in enumerate(target_sheets,1):
                self._log_safe(f"[{i}/{total}] 缓存: {os.path.basename(p)} [{sn}]")
                self._prog_safe(int(i/total*100),f"缓存[{i}/{total}]: {sn}",'determinate')
                try:
                    base=os.path.splitext(p)[0]
                    out_name=f"{base}_{sn}.parquet" if sn!='CSV' else f"{base}.parquet"
                    sheet_param=sn if sn!='CSV' else 0
                    df=read_physical_polars(p,sheet=sheet_param)
                    df.write_parquet(out_name)
                    sz=os.path.getsize(out_name)/1024/1024
                    self._log_safe(f"  完成: {os.path.basename(out_name)} ({df.height}行, {sz:.1f}MB)","success"); ok+=1
                except Exception as e:
                    self._log_safe(f"  失败: {e}","error")
            self._prog_safe(100,"缓存完毕",'determinate')
            self._log_safe(f"单文件缓存完成: {ok}/{total}个Sheet","success")
        threading.Thread(target=worker,daemon=True).start()

    # V50: 批量转缓存（支持指定Sheet）
    def _batch_convert_parquet(self):
        folder=filedialog.askdirectory(title="选择文件夹")
        if not folder: return
        files=scan_data_files(folder)
        to_convert=[f for f in files if not f.lower().endswith('.parquet')]
        if not to_convert: messagebox.showinfo("提示","无需转换"); return
        # V50: 询问Sheet选择模式
        has_excel=any(f.lower().endswith(('.xlsx','.xls')) for f in to_convert)
        target_sheet=0
        if has_excel:
            choice=messagebox.askyesnocancel("Sheet选择",f"共{len(to_convert)}个文件\n\n是=每个文件转全部Sheet\n否=指定Sheet索引或名称\n取消=仅转第一个Sheet")
            if choice is None: target_sheet=0  # 第一个
            elif choice: target_sheet='ALL'
            else:
                sel=simpledialog.askstring("指定Sheet","输入Sheet名称或索引:",initialvalue="0")
                if sel is None: return
                target_sheet=int(sel) if sel.isdigit() else sel
        if not messagebox.askyesno("确认",f"将转换{len(to_convert)}个文件\nSheet模式: {target_sheet}\n继续？"): return
        self._prog_safe(0,"批量转换...",'indeterminate')
        def worker():
            ok=0; fail=0; total=len(to_convert)
            for i,fp in enumerate(to_convert,1):
                self._log_safe(f"[{i}/{total}] {os.path.basename(fp)}")
                self._prog_safe(int(i/total*100),f"转换[{i}/{total}]",'determinate')
                try:
                    if fp.lower().endswith('.csv'):
                        enc=detect_encoding(fp)
                        try: df=pl.read_csv(fp,encoding=enc,ignore_errors=True,infer_schema_length=0)
                        except: df=pl.from_pandas(pd.read_csv(fp,encoding='gbk',on_bad_lines='skip',dtype=str))
                        out=os.path.splitext(fp)[0]+'.parquet'; df.write_parquet(out)
                        self._log_safe(f"  完成: {os.path.basename(out)}","success"); ok+=1
                    else:
                        sheets_to_do=[]
                        if target_sheet=='ALL':
                            sheets_to_do=get_sheet_names(fp)
                        elif isinstance(target_sheet,int):
                            sn=get_sheet_names(fp)
                            sheets_to_do=[sn[min(target_sheet,len(sn)-1)]] if sn else []
                        else:
                            sheets_to_do=[target_sheet]
                        for sn in sheets_to_do:
                            try:
                                df=read_physical_polars(fp,sheet=sn)
                                suffix=f"_{sn}" if len(sheets_to_do)>1 else ""
                                out=os.path.splitext(fp)[0]+suffix+'.parquet'
                                df.write_parquet(out)
                                self._log_safe(f"  [{sn}] 完成: {os.path.basename(out)} ({df.height}行)","success")
                            except Exception as e:
                                self._log_safe(f"  [{sn}] 失败: {e}","error")
                        ok+=1
                except Exception as e: self._log_safe(f"  失败: {e}","error"); fail+=1
            self._prog_safe(100,"转换完毕",'determinate')
            self._log_safe(f"批量转换: 成功{ok}个 失败{fail}个","success")
        threading.Thread(target=worker,daemon=True).start()

    def _open_schema_designer(self):
        mp=self.e_master.get().strip()
        if not os.path.exists(mp): messagebox.showwarning("提示","请先选择主表"); return
        self._prog_safe(0,"读取结构...",'indeterminate')
        def w():
            try:
                if os.path.isdir(mp):
                    files=scan_data_files(mp); t=pick_template_file(files)
                    if not t: raise ValueError("无文件")
                    self.master_template_file=t; cols=get_columns_preview(t,self.folder_sheet)
                else:
                    sh=self.master_sheet or 0
                    if isinstance(self.master_sheet,list): sh=self.master_sheet[0]
                    cols=get_columns_preview(mp,sh)
                self.after(0,lambda:self._open_sd(cols,mp))
            except Exception as e:
                self.after(0,lambda:messagebox.showerror("错误",str(e)))
                self.after(0,lambda:self._prog_safe(0,"Error"))
        threading.Thread(target=w,daemon=True).start()
    def _open_sd(self,cols,mp):
        self._prog_safe(0,"准备就绪",'determinate')
        def cb(sel):
            self.master_schema_cols=sel if sel else None
            self.master_virtual_map={idx_to_letter(i):c for i,c in enumerate(self.master_schema_cols)} if self.master_schema_cols else {}
            self._stash_schema_for(mp)
            self._log_safe(f"主表结构已保存：{len(self.master_schema_cols) if self.master_schema_cols else '全部'}列","success")
        MasterSchemaDesignerDialog(self,cols,selected_cols=self.master_schema_cols,callback=cb)
    def _show_virtual_map(self):
        if not self.master_virtual_map: messagebox.showinfo("提示","未配置结构"); return
        VirtualColumnMapDialog(self,self.master_virtual_map)

    @staticmethod
    def _norm_path(p):
        p=(p or "").strip()
        if not p: return ""
        try: p=os.path.normcase(os.path.abspath(os.path.expanduser(p)))
        except: pass
        return p
    def _make_key(self,p,isf): return f"{'folder' if isf else 'file'}|{self._norm_path(p)}"
    def _stash_current_master_schema(self):
        try: cur=self.e_master.get().strip()
        except: cur=""
        if cur and self.master_schema_cols: self._stash_schema_for(cur)
    def _stash_schema_for(self,p):
        key=self._make_key(p,self.master_is_folder)
        if self.master_schema_cols:
            self.master_schema_by_source[key]=list(self.master_schema_cols)
            mode="folder" if self.master_is_folder else "file"
            self.master_schema_store.setdefault(mode,{})
            self.master_schema_store[mode]["last_cols"]=list(self.master_schema_cols)
            self.master_schema_store[mode]["last_path"]=self._norm_path(p)
        self._master_schema_key=key
    def _restore_master_schema_for(self,p,isf):
        nk=self._make_key(p,isf); mode="folder" if isf else "file"; cols=None
        if nk in self.master_schema_by_source: cols=self.master_schema_by_source[nk]
        elif self.master_schema_store.get(mode,{}).get("last_cols"): cols=self.master_schema_store[mode]["last_cols"]
        self._master_schema_key=nk
        if cols: self.master_schema_cols=list(cols); self.master_virtual_map={idx_to_letter(i):c for i,c in enumerate(cols)}
        else: self.master_schema_cols=None; self.master_virtual_map={}

    def _add_rule(self): RuleDialog(self)
    def _edit_rule(self,e=None):
        sel=self.lst_rules.curselection()
        if sel: RuleDialog(self,sel[0],self.rules[sel[0]])
    def _del_rule(self):
        sel=self.lst_rules.curselection()
        if sel: self.rules.pop(sel[0]); self._refresh_rules()
    def _move_rule(self,d):
        sel=self.lst_rules.curselection()
        if not sel: return
        i=sel[0]; ni=max(0,min(len(self.rules)-1,i+d))
        if i!=ni: self.rules[i],self.rules[ni]=self.rules[ni],self.rules[i]; self._refresh_rules(); self.lst_rules.selection_set(ni)
    def _refresh_rules(self):
        self.lst_rules.delete(0,tk.END)
        for r in self.rules: self.lst_rules.insert(tk.END,f"{os.path.basename(r.target_path)}→{r.pick_cols}")
        self.pool_columns=[]

    def _add_task(self):
        if not self.pool_columns:
            if not self._analyze_pool(): return
        d = TaskEditDialog(self, self.pool_columns, task_count=len(self.tasks))
        self.wait_window(d)
        if d.result:
            self.tasks.append(d.result)
            # V50: 如果用户点了"保存并复制新任务"，同时添加副本
            if hasattr(d, 'dup_result') and d.dup_result:
                self.tasks.append(d.dup_result)
            self._refresh_tasks()

    def _edit_task(self, e=None):
        sel = self.lst_tasks.curselection()
        if not sel: return
        idx = sel[0]
        if not self.pool_columns: self._analyze_pool()
        d = TaskEditDialog(self, self.pool_columns, self.tasks[idx], len(self.tasks))
        self.wait_window(d)
        if d.result:
            self.tasks[idx] = d.result
            # V50: 如果用户点了"保存并复制新任务"，追加副本
            if hasattr(d, 'dup_result') and d.dup_result:
                self.tasks.insert(idx + 1, d.dup_result)
            self._refresh_tasks(idx)

    def _del_task(self):
        sel=self.lst_tasks.curselection()
        if sel: self.tasks.pop(sel[0]); self._refresh_tasks()
    def _move_task(self,d):
        sel=self.lst_tasks.curselection()
        if not sel: return
        i=sel[0]; ni=max(0,min(len(self.tasks)-1,i+d))
        if i!=ni: self.tasks[i],self.tasks[ni]=self.tasks[ni],self.tasks[i]; self._refresh_tasks(ni)
    def _refresh_tasks(self,si=None):
        self.lst_tasks.delete(0,tk.END)
        for t in self.tasks:
            info=f"{len(t.col_map)}列" if not t.pivot_cfg.enabled else "聚合"
            self.lst_tasks.insert(tk.END,f"[{'透视' if t.pivot_cfg.enabled else '明细'}] {t.name} ({info})")
        if si is not None and si<self.lst_tasks.size(): self.lst_tasks.selection_set(si)

    def _analyze_pool(self):
        mp=self.e_master.get().strip()
        if not os.path.exists(mp): self._log_safe("请先选择主表","error"); return False
        self._prog_safe(0,"分析数据池...",'indeterminate')
        try:
            if os.path.isdir(mp):
                self.master_is_folder=True; files=scan_data_files(mp); t=pick_template_file(files)
                if not t: raise ValueError("无文件")
                self.master_template_file=t; tc=get_columns_preview(t,self.folder_sheet)
                if tc and isinstance(tc[0],str) and tc[0].startswith("读取错误:"): raise ValueError(tc[0])
                pool=list(self.master_schema_cols) if self.master_schema_cols else list(tc)
            else:
                self.master_is_folder=False
                sp=self.master_sheet[0] if isinstance(self.master_sheet,list) else self.master_sheet
                bc=get_columns_preview(mp,sp or 0)
                if bc and isinstance(bc[0],str) and bc[0].startswith("读取错误:"): raise ValueError(bc[0])
                pool=list(self.master_schema_cols) if self.master_schema_cols else list(bc)
            src=self.master_schema_cols or pool
            self.master_virtual_map={idx_to_letter(i):c for i,c in enumerate(src)}
            for r in self.rules:
                if not os.path.exists(r.target_path): raise ValueError(f"目标不存在: {r.target_path}")
                tc=get_columns_preview(r.target_path,r.sheet_idx)
                if tc and isinstance(tc[0],str) and tc[0].startswith("读取错误:"): raise ValueError(tc[0])
                for c in resolve_cols_expr(r.pick_cols,tc,r.by_letter,"取回列"):
                    on=f"{r.col_prefix}{c}" if r.col_prefix else c
                    if on not in pool: pool.append(on)
            self.pool_columns=pool
            self._log_safe(f"数据池：{len(pool)}列","success"); self._prog_safe(100,"就绪",'determinate'); return True
        except Exception as e:
            self._log_safe(f"数据池失败: {e}","error"); messagebox.showerror("错误",str(e)); self._prog_safe(0,"Error"); return False

    def _start(self):
        if not self.e_master.get() or not self.tasks: return messagebox.showwarning("提示","请配置主表和任务")
        out=self.e_out.get() or filedialog.asksaveasfilename(defaultextension=".xlsx",filetypes=[("Excel","*.xlsx")])
        if not out: return
        if os.path.exists(out):
            ch=messagebox.askyesnocancel("文件存在","覆盖？\n是=覆盖 | 否=重命名")
            if ch is None: return
            if ch is False: out=unique_path(out)
        self.e_out.delete(0,tk.END); self.e_out.insert(0,out)
        if not self.pool_columns:
            if not self._analyze_pool(): return
        self.btn_start.configure(state='disabled'); self.btn_stop.configure(state='normal',style="Stop.TButton")
        threading.Thread(target=self._worker,args=(self.e_master.get(),out),daemon=True).start()

    def _stop_task(self):
        from core.processor import request_stop; request_stop()
        self.btn_stop.configure(state='disabled'); self._log_safe("已发送停止信号...","warning")
    def _restore_btns(self):
        self.after(0,lambda:self.btn_start.configure(state='normal'))
        self.after(0,lambda:self.btn_stop.configure(state='disabled',style="TButton"))

    def _worker(self,mp,out):
        try:
            execute_pipeline(m_path=mp,out_path=out,rules=list(self.rules),tasks=list(self.tasks),
                             master_sheet=self.master_sheet,master_is_folder=self.master_is_folder,
                             master_schema_cols=self.master_schema_cols,log_func=self._log_safe,
                             prog_func=self._prog_safe,folder_sheet=self.folder_sheet)
            self._restore_btns(); self.msg_queue.put(('finish',None))
        except InterruptedError as e:
            self._log_safe(f"已停止: {e}","warning"); self._prog_safe(0,"已停止",'determinate'); self._restore_btns()
        except PermissionError as e:
            self.msg_queue.put(('error_box',f"文件被占用：{e}")); self._restore_btns()
        except MemoryError as e:
            self._log_safe(f"OOM: {e}","error"); self.msg_queue.put(('error_box',str(e))); self._restore_btns()
        except Exception as e:
            self._log_safe(f"致命错误: {e}","error"); self.msg_queue.put(('error_box',str(e))); self._restore_btns()

    def _on_close(self):
        if messagebox.askokcancel("退出","确定退出？状态将自动保存"): self._save_autosave(); self.master.destroy()
    def _save_autosave(self):
        data={"master":self.e_master.get(),"master_sheet":self.master_sheet,"master_is_folder":self.master_is_folder,
              "master_schema_cols":self.master_schema_cols,"master_template_file":self.master_template_file,
              "master_schema_store":self.master_schema_store,"master_schema_by_source":self.master_schema_by_source,
              "master_schema_key":self._master_schema_key,"output":self.e_out.get(),"folder_sheet":self.folder_sheet,
              "rules":[rule_to_dict(r) for r in self.rules],"tasks":[task_to_dict(t) for t in self.tasks]}
        try:
            with open(AUTOSAVE_FILE,'w',encoding='utf-8') as f: json.dump(data,f,ensure_ascii=False)
        except: pass

    def _auto_save_loop(self):
        """每60秒自动保存当前状态。"""
        try:
            self._save_autosave()
        except Exception:
            pass
        self.after(60000, self._auto_save_loop)

    def _load_autosave(self):
        if not os.path.exists(AUTOSAVE_FILE): return
        try:
            with open(AUTOSAVE_FILE,'r',encoding='utf-8') as f: d=json.load(f)
            self.e_master.insert(0,d.get('master','')); self.e_out.insert(0,d.get('output',''))
            self.master_sheet=d.get('master_sheet',0); self.master_is_folder=bool(d.get('master_is_folder',False))
            self.master_schema_cols=d.get('master_schema_cols'); self.master_template_file=d.get('master_template_file')
            self.master_schema_store=d.get('master_schema_store',self.master_schema_store)
            self.master_schema_by_source=d.get('master_schema_by_source',{})
            self._master_schema_key=d.get('master_schema_key','')
            self.folder_sheet=d.get('folder_sheet',0)
            if self.master_schema_cols: self.master_virtual_map={idx_to_letter(i):c for i,c in enumerate(self.master_schema_cols)}
            self.rules=[MatchRule(**r) for r in d.get('rules',[])]
            self.tasks=[OutputTask.from_dict(t) for t in d.get('tasks',[])]
            self._refresh_rules(); self._refresh_tasks(); self._log_safe("已恢复状态","success")
        except: pass
    def _reset_project(self,confirm=True):
        if confirm and not messagebox.askokcancel("新建","清空所有配置？"): return
        self.e_master.delete(0,tk.END); self.e_out.delete(0,tk.END)
        self.rules=[]; self.tasks=[]; self.pool_columns=[]; self.master_sheet=0
        self.master_schema_cols=None; self.master_virtual_map={}; self.folder_sheet=0
        self._refresh_rules(); self._refresh_tasks(); self.cmb_presets.set(''); self._log_safe("项目已重置")

    def _refresh_presets(self):
        self.cmb_presets['values']=[p['name'] for p in load_presets().get(self.PRESET_KEY,[])]
    def _save_preset(self):
        name=simpledialog.askstring("保存","方案名称:")
        if not name: return
        allp=load_presets()
        if any(p['name']==name for p in allp.get(self.PRESET_KEY,[])):
            if not messagebox.askyesno("已存在",f"方案'{name}'已存在，覆盖？"): return
        data={"master":self.e_master.get(),"master_sheet":self.master_sheet,"master_is_folder":self.master_is_folder,
              "master_schema_cols":self.master_schema_cols,"master_template_file":self.master_template_file,
              "master_schema_store":self.master_schema_store,"master_schema_by_source":self.master_schema_by_source,
              "master_schema_key":self._master_schema_key,"output":self.e_out.get(),"folder_sheet":self.folder_sheet,
              "rules":[rule_to_dict(r) for r in self.rules],"tasks":[task_to_dict(t) for t in self.tasks]}
        lst=[p for p in allp.get(self.PRESET_KEY,[]) if p['name']!=name]
        lst.append({"name":name,"data":data}); allp[self.PRESET_KEY]=lst
        save_presets(allp); self._refresh_presets(); self.cmb_presets.set(name); save_last_active(name)
    def _load_last_active(self):
        name=load_last_active()
        if name and name in (self.cmb_presets['values'] or []): self.cmb_presets.set(name); self._apply_preset(None)
    def _apply_preset(self,e=None):
        name=self.cmb_presets.get()
        for p in load_presets().get(self.PRESET_KEY,[]):
            if p['name']!=name: continue
            d=p['data']; self.e_master.delete(0,tk.END); self.e_master.insert(0,d.get('master',''))
            self.e_out.delete(0,tk.END); self.e_out.insert(0,d.get('output',''))
            self.master_sheet=d.get('master_sheet',0); self.master_is_folder=bool(d.get('master_is_folder',False))
            self.master_schema_cols=d.get('master_schema_cols'); self.master_template_file=d.get('master_template_file')
            self.master_schema_store=d.get('master_schema_store',self.master_schema_store)
            self.master_schema_by_source=d.get('master_schema_by_source',{}); self._master_schema_key=d.get('master_schema_key','')
            self.folder_sheet=d.get('folder_sheet',0)
            if self.master_schema_cols: self.master_virtual_map={idx_to_letter(i):c for i,c in enumerate(self.master_schema_cols)}
            else: self.master_virtual_map={}
            self.rules=[MatchRule(**r) for r in d.get('rules',[])]
            self.tasks=[OutputTask.from_dict(t) for t in d.get('tasks',[])]
            self._refresh_rules(); self._refresh_tasks(); self._log_safe(f"已加载: {name}"); save_last_active(name); return
    def _del_preset(self):
        name=self.cmb_presets.get()
        if name and messagebox.askyesno("删除",f"删除'{name}'？"):
            allp=load_presets(); allp[self.PRESET_KEY]=[p for p in allp.get(self.PRESET_KEY,[]) if p['name']!=name]
            save_presets(allp); self._refresh_presets(); self.cmb_presets.set('')
    def _export_preset(self):
        name=self.cmb_presets.get()
        if not name: messagebox.showwarning("提示","请选方案"); return
        for p in load_presets().get(self.PRESET_KEY,[]):
            if p['name']==name:
                out=filedialog.asksaveasfilename(defaultextension=".json",filetypes=[("方案","*.json")],initialfile=f"DataLink方案_{name}.json")
                if not out: return
                with open(out,'w',encoding='utf-8') as f: json.dump(p,f,ensure_ascii=False,indent=2)
                self._log_safe(f"方案已导出: {out}","success")
                try:
                    if os.name=='nt': subprocess.run(f'explorer /select,"{os.path.normpath(out)}"',shell=True)
                except: pass
                return
    def _import_preset(self):
        inp=filedialog.askopenfilename(filetypes=[("方案","*.json")],title="导入方案")
        if not inp: return
        try:
            with open(inp,'r',encoding='utf-8') as f: preset=json.load(f)
            if 'name' not in preset or 'data' not in preset: messagebox.showerror("格式错误","无效方案文件"); return
            name=preset['name']; allp=load_presets()
            if any(p['name']==name for p in allp.get(self.PRESET_KEY,[])):
                if not messagebox.askyesno("已存在",f"方案'{name}'已存在，覆盖？"):
                    nn=simpledialog.askstring("重命名","新名称:",initialvalue=f"{name}_导入")
                    if not nn: return
                    preset['name']=nn; name=nn
            lst=[p for p in allp.get(self.PRESET_KEY,[]) if p['name']!=name]
            lst.append(preset); allp[self.PRESET_KEY]=lst; save_presets(allp)
            self._refresh_presets(); self.cmb_presets.set(name); self._apply_preset(None)
            self._log_safe(f"方案已导入: {name}","success")
        except json.JSONDecodeError: messagebox.showerror("错误","无效JSON")
        except Exception as e: messagebox.showerror("错误",str(e))
