"""V50 核心业务对话框。"""
import os, re, copy, threading, tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog
from typing import List, Optional
import pandas as pd, numpy as np
from ui.theme import THEME
from ui.components import AppleDialog, SheetPickerDialog, SmartColumnPicker, MultiSortDialog
from core.parser import split_smart, idx_to_letter, split_plus
from core.models import MatchRule, OutputTask, PivotConfig
from core.io_engine import get_sheet_names, get_columns_preview

FORMULA_HELP = """
# DataLink Pro 公式参考大全 (50例)

## 使用说明

1. 复制公式到"计算字段"输入框
2. 将中文列名替换为你表格里真实的列名，格式：`df['列名']`
3. 新定义的计算列名不需要 `df['列名']` 格式，直接写列名
4. 多个新增字段公式用英文逗号分隔，或者回车换行
5. 程序逻辑：先计算 → 再筛选，且计算字段从左到右，从上到下依次执行
6. 计算后的字段可用于筛选条件
7. 前面的计算字段可被后面的计算字段引用，前计算新增字段用于后计算字段时，需要 `df['列名']` 格式

> ★★★ 定义原表字段必须用 `df['列名']` 格式，例：`df['物料编码']` ★★★

---

## 第一章：新增列与空列

```
1. 备注
2. 备注, 原因, 跟进状态
3. 状态 = '正常'
4. 检查标识 = 1
```

## 第二章：基础数学运算

> 数值建议加上 `.astype(float)`、.astype(int) 防止文本列报错

```
5.  总分 = df['语文'].astype(int) + df['数学'].astype(int)
6.  利润 = df['销售额'].astype(float) - df['成本'].astype(float)
7.  总金额 = df['单价'].astype(float) * df['数量'].astype(int)
8.  均价 = df['总金额'].astype(float) / df['数量'].astype(int)
9.  含税价 = df['未税价'].astype(float) * 1.13
10. 折后价 = df['原价'].astype(float) * 0.8
11. 绩效 = (df['基本工资'].astype(int) + df['提成'].astype(int)) * 1.2
12. 余数 = df['序号'] % 2
```

## 第三章：文本处理

> 建议加上 `.astype(str)` 防止数字列报错

```
13. 全名 = df['姓'] + df['名']
14. 唯一码 = df['店号'].astype(str) + '-' + df['商品ID'].astype(str)
15. 省份 = df['身份证号'].astype(str).str[:3]
16. 尾号 = df['手机号'].astype(str).str[-4:]
17. 代码 = df['编号'].astype(str).str[2:5]
18. 英文名 = df['英文名'].str.upper()
19. 邮箱 = df['邮箱'].str.lower()
20. 清洗后姓名 = df['姓名'].str.strip()
21. 城市代码 = df['城市'].str.replace('北京', 'BJ')
22. 是否退货 = df['备注'].str.contains('退货')
23. 字数 = df['备注'].str.len()
```

## 第四章：逻辑判断

>涉及运算数值建议加上 `.astype(float).fillna(0)`、.astype(int).fillna(0) 防止空列、文本列报错

```
24. 状态 = np.where(df['库存'] < 10, '缺货', '充足')
25. 等级 = np.where(df['销量'] > 1000, '一级', np.where(df['销量'] > 50, '二级', '三级'))
26. 是否VIP = np.where(df['会员类型'] == '金卡', '是', '否')
27. 重点关注 = np.where((df['销量'].astype(float).fillna(0) > 100) & (df['利润'] < 0), '是', '否')
28. 需要补货 = np.where((df['库存'] < 10) | (df['日均销'] > 50), '是', '否')
29. 备注 = df['备注'].fillna('无')
30. 填写状态 = np.where(df['手机号'].isna(), '未填', '已填')
```

## 第五章：日期与时间

```
31. 日期格式化 = pd.to_datetime(df['日期列'])
32. 年 = pd.to_datetime(df['日期列']).dt.year
33. 月 = pd.to_datetime(df['日期列']).dt.month
34. 日 = pd.to_datetime(df['日期列']).dt.day
35. 年月 = df['日期'].dt.strftime('%Y-%m')
36. 星期 = pd.to_datetime(df['日期列']).dt.dayofweek
37. 缺货天数 = (pd.Timestamp.now() - pd.to_datetime(df['首次缺货日期'])).dt.days
38. 间隔 = (pd.to_datetime(df['结束日期']) - pd.to_datetime(df['开始日期'])).dt.days.fillna(0)
```

## 第六章：数据清洗与转换

```
39. 销量 = df['销量'].fillna(0)
40. 数量 = df['数量'].astype(int)
41. 金额 = df['金额'].astype(float)
42. 文本ID = df['ID'].astype(str)
43. 金额 = round(df['金额'], 2)
44. 箱数 = np.ceil(df['总数'] / 每箱数量)
45. 整箱 = np.floor(df['总数'] / 每箱数量)
```

## 第七章：进阶技巧

```
46. 发货地 = df['实际发货地'].fillna(df['默认发货地'])
47. 摘要 = df['详情'].astype(str).str[:10]
48. 脱敏手机 = df['手机'].astype(str).str[:3] + '****' + df['手机'].astype(str).str[-4:]
49. 性别码 = df['身份证'].astype(str).str[-2].astype(int)
    性别 = np.where(df['性别码'] % 2 == 1, '男', '女')
50. 出生年 = df['身份证'].astype(str).str[6:10]
```

---

更多高级用法请查阅 [Pandas 官方文档](https://pandas.pydata.org/docs/)

"""

class HelpDialog(AppleDialog):
    def __init__(self,parent):
        super().__init__(parent,"公式参考")
        self.geometry("700x600")
        txt=scrolledtext.ScrolledText(self,font=THEME['font_mono_lg'],padx=15,pady=15,relief="flat")
        txt.pack(fill='both',expand=True); txt.insert('1.0',FORMULA_HELP); txt.configure(state='disabled')

class RuleDialog(AppleDialog):
    def __init__(self,parent,idx=None,rule=None):
        super().__init__(parent,"匹配规则配置")
        self.parent_frame=parent; self.idx=idx
        sw=min(800,int(self.winfo_screenwidth()*0.8)); sh=min(600,int(self.winfo_screenheight()*0.8))
        self.geometry(f"{sw}x{sh}"); self._ui(rule)
    def _ui(self,rule):
        f=ttk.Frame(self,padding=20); f.pack(fill='both',expand=True); f.columnconfigure(1,weight=1)
        ttk.Label(f,text="目标文件:").grid(row=0,column=0,sticky='e')
        self.e_path=ttk.Entry(f); self.e_path.grid(row=0,column=1,sticky='ew',padx=5)
        ttk.Button(f,text="选择",width=6,command=self._sel).grid(row=0,column=2)
        ttk.Label(f,text="Sheet:").grid(row=1,column=0,sticky='e')
        fs=ttk.Frame(f); fs.grid(row=1,column=1,sticky='ew',padx=5)
        self.e_sheet=ttk.Entry(fs,width=10)
        ds=0
        if rule and rule.sheet_idx is not None: ds=rule.sheet_idx
        self.e_sheet.insert(0,str(ds)); self.e_sheet.pack(side='left')
        ttk.Button(fs,text="选择Sheet",command=self._pick_sh).pack(side='left',padx=2)
        ttk.Button(fs,text="目标列名",command=self._show_cols).pack(side='left',padx=2)
        ttk.Button(fs,text="主表列名",command=lambda:getattr(self.parent_frame,'_show_virtual_map',lambda:None)()).pack(side='left',padx=2)
        self.var_mode=tk.BooleanVar(value=True)
        ttk.Checkbutton(f,text="列名匹配(不勾选则用字母A,B,C:F)",variable=self.var_mode).grid(row=2,column=1,sticky='w',pady=10)
        def ar(r,txt,attr):
            ttk.Label(f,text=txt).grid(row=r,column=0,sticky='e',pady=5)
            e=ttk.Entry(f); e.grid(row=r,column=1,columnspan=2,sticky='ew',padx=5); setattr(self,attr,e)
        ar(3,"主表关联列:","e_m"); ar(4,"目标关联列:","e_t"); ar(5,"目标取回列:","e_p"); ar(6,"列名前缀:","e_pre")
        ttk.Button(f,text="保存配置",command=self._ok,style="Accent.TButton").grid(row=7,column=1,pady=20)
        if rule:
            self.e_path.insert(0,rule.target_path); self.var_mode.set(not rule.by_letter)
            self.e_m.insert(0,rule.master_keys); self.e_t.insert(0,rule.target_keys)
            self.e_p.insert(0,rule.pick_cols); self.e_pre.insert(0,rule.col_prefix)
    def _sel(self):
        p=filedialog.askopenfilename(filetypes=[("Data","*.xlsx *.xls *.csv *.parquet")])
        if p:
            self.e_path.delete(0,tk.END); self.e_path.insert(0,p)
            if p.lower().endswith(('.xlsx','.xls')) and self.e_sheet.get()=='0':
                ns=get_sheet_names(p)
                if ns: self.e_sheet.delete(0,tk.END); self.e_sheet.insert(0,ns[0])
            if not self.e_pre.get(): self.e_pre.insert(0,os.path.splitext(os.path.basename(p))[0]+"_")
    def _pick_sh(self):
        p=self.e_path.get()
        if os.path.exists(p): SheetPickerDialog(self,get_sheet_names(p),lambda s:(self.e_sheet.delete(0,tk.END),self.e_sheet.insert(0,s)))
    def _show_cols(self):
        p=self.e_path.get()
        if not os.path.exists(p): messagebox.showwarning("提示","路径不存在"); return
        sh=self.e_sheet.get(); sheet=int(sh) if sh.isdigit() else sh
        def w():
            try:
                cols=get_columns_preview(p,sheet)
                self.after(0,lambda:__import__('ui.components',fromlist=['ColumnViewer']).ColumnViewer(self,p,sheet,cols=cols))
            except Exception as e: self.after(0,lambda:messagebox.showerror("错误",str(e)))
        threading.Thread(target=w,daemon=True).start()
    def _ok(self):
        try:
            sv=self.e_sheet.get(); sheet=int(sv) if sv.isdigit() else sv
            r=MatchRule(self.e_path.get(),sheet,not self.var_mode.get(),self.e_m.get().strip(),self.e_t.get().strip(),self.e_p.get().strip(),self.e_pre.get().strip())
            if not r.target_path or not r.master_keys or not r.target_keys or not r.pick_cols:
                return messagebox.showwarning("提示","请填写完整")
            if self.idx is not None: self.parent_frame.rules[self.idx]=r
            else: self.parent_frame.rules.append(r)
            self.parent_frame._refresh_rules(); self.destroy()
        except Exception as e: messagebox.showerror("Error",str(e))

class TaskEditDialog(AppleDialog):
    def __init__(self,parent,all_columns,task=None,task_count=0):
        super().__init__(parent,"配置输出任务")
        sw=min(1200,int(self.winfo_screenwidth()*0.85)); sh=min(1000,int(self.winfo_screenheight()*0.85))
        self.geometry(f"{sw}x{sh}")
        self.physical_cols=list(dict.fromkeys(all_columns)); self.calc_cols=[]; self.result=None; self.task_count=task_count
        self._ui(task); self._parse_calc_cols(); self._refresh_src_list()
    def _get_all_cols(self): return list(dict.fromkeys(self.physical_cols+self.calc_cols))
    def _ui(self,task):
        main=ttk.Frame(self,padding=20); main.pack(fill="both",expand=True)
        fi=ttk.LabelFrame(main,text="基础配置",padding=15); fi.pack(fill='x',pady=5)
        ttk.Label(fi,text="Sheet名称:").grid(row=0,column=0,sticky='e')
        self.e_name=ttk.Entry(fi,width=40); self.e_name.grid(row=0,column=1,sticky='w',padx=5)
        if not task: self.e_name.insert(0,f"Sheet{self.task_count+1}")
        ttk.Label(fi,text="筛选条件:").grid(row=0,column=2,sticky='e')
        ff=ttk.Frame(fi); ff.grid(row=0,column=3,sticky='w',padx=5)
        self.e_filter=ttk.Entry(ff,width=48); self.e_filter.pack(side='left')
        ttk.Button(ff,text="构建",width=5,command=self._open_fb).pack(side='left',padx=3)
        ttk.Label(fi,text="计算字段:").grid(row=1,column=0,sticky='ne',pady=5)
        self.e_calc=tk.Text(fi,width=130,height=8,font=THEME['font_mono'])
        self.e_calc.grid(row=1,column=1,columnspan=3,sticky='w',padx=5,pady=5)
        self.e_calc.bind("<KeyRelease>",self._parse_calc_cols)
        ft=ttk.Frame(fi); ft.grid(row=2,column=1,columnspan=3,sticky='w',padx=5)
        ttk.Button(ft,text="插入列名",command=self._ins_col).pack(side='left',padx=2)
        ttk.Button(ft,text="语法检查",command=self._dry).pack(side='left',padx=2)
        ttk.Button(fi,text="公式参考",command=lambda:HelpDialog(self)).grid(row=2,column=2,sticky='e',padx=2)
        self.sort_data=[]
        self.lbl_sort=ttk.Label(fi,text=self._fmt_sort(),foreground=THEME['accent'])
        self.lbl_sort.grid(row=3,column=1,sticky='w',padx=5)
        ttk.Button(fi,text="设置排序",command=self._open_sort).grid(row=3,column=0,sticky='e')
        self.nb=ttk.Notebook(main); self.nb.pack(fill='both',expand=True,pady=10)
        self.tab_d=ttk.Frame(self.nb,padding=10); self.nb.add(self.tab_d,text=" 明细表 ")
        self._init_detail()
        self.tab_p=ttk.Frame(self.nb,padding=10); self.nb.add(self.tab_p,text=" 透视表 ")
        self._init_pivot()
        fb=ttk.Frame(main); fb.pack(fill='x',pady=10)
        ttk.Button(fb, text="保存并复制新任务", command=self._dup_and_save).pack(side='left')
        ttk.Button(fb,text="保存配置",command=self._save,style="Accent.TButton").pack(side='right',ipadx=20)
        if task:
            self.e_name.delete(0,tk.END); self.e_name.insert(0,task.name)
            self.e_filter.delete(0,tk.END); self.e_filter.insert(0,task.filter_expr)
            self.e_calc.delete('1.0',tk.END); self.e_calc.insert('1.0',task.calc_fields)
            self.sort_data=task.sort_config; self.lbl_sort.config(text=self._fmt_sort())
            for item in task.col_map: self.tv_dst.insert("","end",values=(item['src'],item['dst']))
            pc=task.pivot_cfg
            if pc.enabled:
                self.nb.select(self.tab_p)
                self.e_grp.delete(0,tk.END); self.e_grp.insert(0,','.join(pc.group_by))
                self.e_pvt.delete(0,tk.END); self.e_pvt.insert(0,','.join(pc.pivot_cols))
                self.e_val.delete(0,tk.END); self.e_val.insert(0,','.join([f"{k}:{v}" for k,v in pc.values_agg.items()]))
                self.e_renames.delete(0,tk.END); self.e_renames.insert(0,pc.renames)

    def _init_detail(self):
        paned=ttk.PanedWindow(self.tab_d,orient='horizontal'); paned.pack(fill='both',expand=True)
        fl=ttk.LabelFrame(paned,text="可用数据源",padding=5); paned.add(fl,weight=1)
        fss=ttk.Frame(fl); fss.pack(fill='x',pady=(0,3))
        ttk.Label(fss,text="搜索:").pack(side='left')
        self.var_src_s=tk.StringVar(); self.var_src_s.trace_add("write",lambda *_:self._refresh_src_list())
        ttk.Entry(fss,textvariable=self.var_src_s).pack(side='left',fill='x',expand=True,padx=3)
        sb=ttk.Scrollbar(fl); sb.pack(side='right',fill='y')
        self.lb_src=tk.Listbox(fl,selectmode='extended',relief="flat",bg=THEME['input_bg'],yscrollcommand=sb.set)
        self.lb_src.pack(fill='both',expand=True); sb.config(command=self.lb_src.yview)
        self.lb_src.bind("<Double-1>",self._add_col)
        fm=ttk.Frame(paned); paned.add(fm,weight=0)
        for t,c in [("添加>",self._add_col),("移除<",self._del_col)]:
            ttk.Button(fm,text=t,command=c).pack(pady=5,padx=5)
        ttk.Separator(fm,orient='horizontal').pack(fill='x',pady=5)
        for t,c in [("置顶",self._mt),("置底",self._mb)]:
            ttk.Button(fm,text=t,command=c).pack(pady=2,padx=5)
        ttk.Separator(fm,orient='horizontal').pack(fill='x',pady=5)
        for t,c in [("全选>>",self._add_all),("清空<<",self._clr_all)]:
            ttk.Button(fm,text=t,command=c).pack(pady=5,padx=5)
        fr=ttk.LabelFrame(paned,text="输出列&顺序",padding=5); paned.add(fr,weight=2)
        sbr=ttk.Scrollbar(fr); sbr.pack(side='right',fill='y')
        self.tv_dst=ttk.Treeview(fr,columns=("源列","新列名"),show='headings',yscrollcommand=sbr.set)
        self.tv_dst.heading("源列",text="源列"); self.tv_dst.column("源列",width=180)
        self.tv_dst.heading("新列名",text="新列名(双击改)"); self.tv_dst.column("新列名",width=180)
        self.tv_dst.pack(fill='both',expand=True); sbr.config(command=self.tv_dst.yview)
        self.tv_dst.bind("<ButtonPress-1>",self._on_press)
        self.tv_dst.bind("<B1-Motion>",self._on_motion)
        self.tv_dst.bind("<ButtonRelease-1>",self._on_release)
        self.tv_dst.bind("<Double-1>",self._on_edit)
        self._drag={"item":None}
        self.tv_dst.bind("<Button-3>",self._on_rc)
        self._smenu=tk.Menu(self.tv_dst,tearoff=0)
        self._smenu.add_command(label="按此列升序",command=lambda:self._qsort(True))
        self._smenu.add_command(label="按此列降序",command=lambda:self._qsort(False))
        self._smenu.add_separator()
        self._smenu.add_command(label="选中项倒序",command=self._rev_sel)
        self._smenu.add_command(label="清除所有排序",command=self._clr_sort)
        self._rc_col=None

    def _init_pivot(self):
        f=ttk.Frame(self.tab_p); f.pack(fill='x',padx=20,pady=20)
        def cr(r,label,attr,tip,multi=True,ac=False):
            ttk.Label(f,text=label).grid(row=r,column=0,sticky='e',pady=10)
            ent=ttk.Entry(f,width=60); ent.grid(row=r,column=1,sticky='w',padx=10); setattr(self,attr,ent)
            def cb(res):
                val=",".join(res) if isinstance(res,list) else res
                if ac and val: val=",".join([f"{x}:sum" if ':' not in x else x for x in split_smart(val)])
                cur=ent.get().strip()
                if cur: val=",".join(list(set(split_smart(cur)).union(set(split_smart(val)))))
                ent.delete(0,tk.END); ent.insert(0,val)
            ttk.Button(f,text="选列...",command=lambda:SmartColumnPicker(self,self._get_all_cols(),cb,multi)).grid(row=r,column=2,sticky='w')
            ttk.Label(f,text=tip,foreground=THEME['fg_secondary']).grid(row=r,column=3,sticky='w',padx=10)
        cr(0,"行标签:","e_grp","例: 科室,计划员")
        cr(1,"列标签:","e_pvt","例: 年份(可选)")
        cr(2,"值:聚合:","e_val","例: 数量:sum",ac=True)
        ttk.Label(f,text="重命名:").grid(row=3,column=0,sticky='e',pady=10)
        self.e_renames=ttk.Entry(f,width=60); self.e_renames.grid(row=3,column=1,sticky='w',padx=10)
        ttk.Label(f,text="原列=新列(批量替换)",foreground=THEME['fg_secondary']).grid(row=3,column=3,sticky='w',padx=10)

    # drag
    def _on_press(self,e):
        item=self.tv_dst.identify_row(e.y)
        if item: self._drag["item"]=item
    def _on_motion(self,e):
        t=self.tv_dst.identify_row(e.y)
        if t: self.tv_dst.selection_set(t)
    def _on_release(self,e):
        t=self.tv_dst.identify_row(e.y); s=self._drag["item"]
        if s and t and s!=t: self.tv_dst.move(s,self.tv_dst.parent(s),self.tv_dst.index(t))
    def _on_edit(self,e):
        col=self.tv_dst.identify_column(e.x); item=self.tv_dst.identify_row(e.y)
        if col=="#2" and item:
            bbox=self.tv_dst.bbox(item,column="#2")
            if not bbox: return
            ent=tk.Entry(self.tv_dst); ent.place(x=bbox[0],y=bbox[1],w=bbox[2],h=bbox[3])
            ent.insert(0,self.tv_dst.item(item,"values")[1]); ent.select_range(0,tk.END); ent.focus()
            def sv(_):
                vs=list(self.tv_dst.item(item,"values")); vs[1]=ent.get(); self.tv_dst.item(item,values=vs); ent.destroy()
            ent.bind("<Return>",sv); ent.bind("<FocusOut>",sv)
    # col ops
    def _add_col(self,e=None):
        for i in self.lb_src.curselection(): self.tv_dst.insert("","end",values=(self.lb_src.get(i).replace(" (计算)",""),)*2)
    def _add_all(self):
        for i in range(self.lb_src.size()): self.tv_dst.insert("","end",values=(self.lb_src.get(i).replace(" (计算)",""),)*2)
    def _del_col(self):
        for i in self.tv_dst.selection(): self.tv_dst.delete(i)
    def _clr_all(self):
        for i in self.tv_dst.get_children(): self.tv_dst.delete(i)
    def _mt(self):
        for item in reversed(self.tv_dst.selection()): self.tv_dst.move(item,'',0)
    def _mb(self):
        n=len(self.tv_dst.get_children())
        for item in self.tv_dst.selection(): self.tv_dst.move(item,'',n)
    # right click
    def _on_rc(self,e):
        item=self.tv_dst.identify_row(e.y)
        if not item: return
        vs=self.tv_dst.item(item,"values")
        if vs: self._rc_col=vs[0]; self._smenu.post(e.x_root,e.y_root)
    def _qsort(self,asc):
        if not self._rc_col: return
        self.sort_data=[s for s in self.sort_data if s['col']!=self._rc_col]
        self.sort_data.insert(0,{'col':self._rc_col,'asc':asc}); self.lbl_sort.config(text=self._fmt_sort()); self._rc_col=None
    def _rev_sel(self):
        sel=list(self.tv_dst.selection())
        if len(sel)<2: return
        idxs=[self.tv_dst.index(s) for s in sel]; vals=[self.tv_dst.item(s,"values") for s in sel]; vals.reverse()
        for s in sel: self.tv_dst.delete(s)
        sidxs=sorted(idxs); new=[]
        for i,idx in enumerate(sidxs): new.append(self.tv_dst.insert("",idx,values=vals[i]))
        for item in new: self.tv_dst.selection_add(item)
    def _clr_sort(self): self.sort_data=[]; self.lbl_sort.config(text=self._fmt_sort())
    # calc
    def _parse_calc_cols(self,e=None):
        txt=self.e_calc.get("1.0",tk.END); found=[]
        for p in split_smart(txt):
            p=p.strip()
            if not p: continue
            if '=' in p:
                lhs=p.split('=',1)[0].strip(); m=re.search(r"df\s*\[\s*['\"]([^'\"]+)['\"]\s*\]",lhs)
                found.append(m.group(1) if m else lhs)
            else: found.append(p)
        kw={'df','pd','np','if','else','for','while','True','False','None'}
        self.calc_cols=[c for c in sorted(set(found)) if c not in kw and c not in self.physical_cols]
        self._refresh_src_list()
    def _refresh_src_list(self):
        key=""
        if hasattr(self,'var_src_s'): key=self.var_src_s.get().strip().lower()
        self.lb_src.delete(0,tk.END)
        for c in self.physical_cols:
            if key and key not in str(c).lower(): continue
            self.lb_src.insert(tk.END,c)
        for c in self.calc_cols:
            if key and key not in str(c).lower(): continue
            self.lb_src.insert(tk.END,f"{c} (计算)"); self.lb_src.itemconfig(tk.END,{'fg':THEME['accent']})
    def _ins_col(self):
        sel=self.lb_src.curselection()
        if sel: self.e_calc.insert(tk.INSERT,f"df['{self.lb_src.get(sel[0]).replace(' (计算)','')}']"); self.e_calc.focus()
    def _dry(self):
        txt=self.e_calc.get("1.0",tk.END).strip()
        if not txt: return messagebox.showinfo("检查","没有公式")
        mock={c:[0] for c in self.physical_cols}
        try:
            df=pd.DataFrame(mock)
            for c in df.columns: df[c]=df[c].astype(str)
            for p in split_smart(txt):
                p=p.strip()
                if not p: continue
                if '=' in p:
                    lhs,rhs=p.split('=',1); lhs=lhs.strip()
                    cmd=f"{lhs}={rhs.strip()}" if lhs.startswith('df[') else f"df['{lhs}']={rhs.strip()}"
                else: cmd=f"df['{p}']=''"; exec(cmd,{'df':df,'pd':pd,'np':np})
            messagebox.showinfo("成功","语法检查通过!")
        except Exception as e: messagebox.showerror("错误",str(e))
    def _fmt_sort(self):
        if not self.sort_data: return "当前: 无排序"
        return "当前: "+", ".join([f"{x['col']}({'升' if x['asc'] else '降'})" for x in self.sort_data])
    def _open_sort(self):
        d=MultiSortDialog(self,self._get_all_cols(),self.sort_data); self.wait_window(d)
        if d.result is not None: self.sort_data=d.result; self.lbl_sort.config(text=self._fmt_sort())
    def _open_fb(self):
        def cb(expr): self.e_filter.delete(0,tk.END); self.e_filter.insert(0,expr)
        FilterBuilderDialog(self,self._get_all_cols(),self.e_filter.get().strip(),cb)
    def _dup(self):
        """复制为新任务：先保存当前任务，再创建一个副本任务。"""
        # 先保存当前任务（防止用户配置丢失）
        self._save()
        # _save成功后self.result已赋值且窗口已destroy
        # 需要通知父级再打开一个预填的副本
        # 所以改为：不destroy，而是标记需要复制
        pass

    def _dup_and_save(self):
        """先保存当前，再自动新建副本。"""
        name = self.e_name.get().strip()
        if not name:
            return messagebox.showwarning("提示", "Sheet名称必填")

        # 1. 先保存当前任务
        mode = self.nb.index(self.nb.select())
        cm = []
        pc = PivotConfig()
        if mode == 0:
            for item in self.tv_dst.get_children():
                vs = self.tv_dst.item(item, "values")
                cm.append({'src': vs[0], 'dst': vs[1]})
            if not cm:
                return messagebox.showwarning("提示", "请添加列")
        else:
            grp = split_smart(self.e_grp.get())
            pvt = split_smart(self.e_pvt.get())
            vr = split_smart(self.e_val.get())
            va = {}
            for v in vr:
                if ':' in v:
                    k, fn = v.split(':')
                    va[k.strip()] = fn.strip()
                else:
                    va[v] = 'sum'
            if not grp and not va:
                return messagebox.showwarning("提示", "需要分组和值")
            pc = PivotConfig(enabled=True, group_by=grp, pivot_cols=pvt,
                             values_agg=va, renames=self.e_renames.get().strip())

        # 保存当前任务
        current = OutputTask(
            name=name,
            filter_expr=self.e_filter.get().strip(),
            calc_fields=self.e_calc.get("1.0", tk.END).strip(),
            sort_config=self.sort_data, col_map=cm, pivot_cfg=pc,
        )
        self.result = current

        # 2. 同时创建副本任务
        import copy
        dup = copy.deepcopy(current)
        dup.name = f"{name}_副本"
        self.dup_result = dup

        self.destroy()

    def _save(self):
        name=self.e_name.get().strip()
        if not name: return messagebox.showwarning("提示","Sheet名称必填")
        mode=self.nb.index(self.nb.select()); cm=[]; pc=PivotConfig()
        if mode==0:
            for item in self.tv_dst.get_children():
                vs=self.tv_dst.item(item,"values"); cm.append({'src':vs[0],'dst':vs[1]})
            if not cm: return messagebox.showwarning("提示","请添加列")
        else:
            grp=split_smart(self.e_grp.get()); pvt=split_smart(self.e_pvt.get())
            vr=split_smart(self.e_val.get()); va={}
            for v in vr:
                if ':' in v: k,fn=v.split(':'); va[k.strip()]=fn.strip()
                else: va[v]='sum'
            if not grp and not va: return messagebox.showwarning("提示","需要分组和值")
            pc=PivotConfig(enabled=True,group_by=grp,pivot_cols=pvt,values_agg=va,renames=self.e_renames.get().strip())
        self.result=OutputTask(name=name,filter_expr=self.e_filter.get().strip(),calc_fields=self.e_calc.get("1.0",tk.END).strip(),
                               sort_config=self.sort_data,col_map=cm,pivot_cfg=pc)
        self.destroy()

class FilterBuilderDialog(AppleDialog):
    OPERATORS=[("等于","==","single"),("不等于","!=","single"),("大于",">","single"),("大于等于",">=","single"),
               ("小于","<","single"),("小于等于","<=","single"),("包含文本","str.contains","single"),
               ("不包含文本","~str.contains","single"),("以...开头","str.startswith","single"),
               ("以...结尾","str.endswith","single"),("在列表中(in)","isin","multi"),
               ("不在列表中(not in)","~isin","multi"),("区间范围","between","range"),
               ("为空","isna","none"),("不为空","notna","none")]
    def __init__(self,parent,all_cols,current_expr="",callback=None):
        super().__init__(parent,"筛选条件构建器")
        sw=min(800,int(self.winfo_screenwidth()*0.8)); sh=min(700,int(self.winfo_screenheight()*0.8))
        self.geometry(f"{sw}x{sh}"); self.all_cols=all_cols; self.cb=callback; self.conditions=[]; self._ui(current_expr)
        self.lift(); self.focus_force()
    def _ui(self,ce):
        f=ttk.Frame(self,padding=15); f.pack(fill='both',expand=True)
        fa=ttk.LabelFrame(f,text="添加条件",padding=10); fa.pack(fill='x',pady=(0,10))
        r1=ttk.Frame(fa); r1.pack(fill='x',pady=5)
        ttk.Label(r1,text="列:").pack(side='left')
        self.cmb_col=ttk.Combobox(r1,values=self.all_cols,state='readonly',width=25); self.cmb_col.pack(side='left',padx=5)
        ttk.Label(r1,text="运算:").pack(side='left',padx=(10,0))
        self.cmb_op=ttk.Combobox(r1,values=[o[0] for o in self.OPERATORS],state='readonly',width=18)
        self.cmb_op.pack(side='left',padx=5); self.cmb_op.current(0); self.cmb_op.bind("<<ComboboxSelected>>",self._oc)
        self.fv=ttk.Frame(fa); self.fv.pack(fill='x',pady=5)
        self.fs=ttk.Frame(self.fv); ttk.Label(self.fs,text="值:").pack(side='left')
        self.e_val=ttk.Entry(self.fs,width=30); self.e_val.pack(side='left',padx=5)
        self.fm=ttk.Frame(self.fv); ttk.Label(self.fm,text="值列表(每行一个/逗号分隔/Excel粘贴):").pack(anchor='w')
        self.txt_m=tk.Text(self.fm,width=60,height=5,font=("Consolas",9)); self.txt_m.pack(fill='x',pady=3)
        self.fr=ttk.Frame(self.fv)
        ttk.Label(self.fr,text="最小:").pack(side='left'); self.e_min=ttk.Entry(self.fr,width=12); self.e_min.pack(side='left',padx=5)
        ttk.Label(self.fr,text="最大:").pack(side='left',padx=(10,0)); self.e_max=ttk.Entry(self.fr,width=12); self.e_max.pack(side='left',padx=5)
        self.fs.pack(fill='x')
        ttk.Button(fa,text="添加条件",command=self._add,style="Accent.TButton").pack(anchor='e',pady=5)
        fl=ttk.LabelFrame(f,text="条件列表(双击删除)",padding=10); fl.pack(fill='both',expand=True,pady=(0,10))
        flg=ttk.Frame(fl); flg.pack(fill='x',pady=(0,5))
        ttk.Label(flg,text="关系:").pack(side='left')
        self.var_logic=tk.StringVar(value="AND")
        ttk.Radiobutton(flg,text="全部满足(AND)",variable=self.var_logic,value="AND",command=self._upd).pack(side='left',padx=10)
        ttk.Radiobutton(flg,text="满足任一(OR)",variable=self.var_logic,value="OR",command=self._upd).pack(side='left',padx=10)
        self.tv=ttk.Treeview(fl,columns=("列","运算","值","表达式"),show='headings',height=6)
        for c,w in [("列",120),("运算",120),("值",180),("表达式",300)]:
            self.tv.heading(c,text=c); self.tv.column(c,width=w)
        self.tv.pack(fill='both',expand=True); self.tv.bind("<Double-1>",self._delc)
        fp=ttk.LabelFrame(f,text="最终表达式(可手动修改)",padding=10); fp.pack(fill='x',pady=(0,10))
        self.e_res=ttk.Entry(fp,width=90); self.e_res.pack(fill='x')
        if ce: self.e_res.insert(0,ce)
        fb=ttk.Frame(f); fb.pack(fill='x')
        ttk.Button(fb,text="清空",command=self._clr).pack(side='left')
        ttk.Button(fb,text="确认",command=self._ok,style="Accent.TButton").pack(side='right',padx=5)
    def _oc(self,e=None):
        op=self.cmb_op.get(); ot="single"
        for n,c,t in self.OPERATORS:
            if n==op: ot=t; break
        self.fs.pack_forget(); self.fm.pack_forget(); self.fr.pack_forget()
        if ot=="single": self.fs.pack(fill='x')
        elif ot=="multi": self.fm.pack(fill='x')
        elif ot=="range": self.fr.pack(fill='x')
    def _pmv(self):
        raw=self.txt_m.get("1.0",tk.END).strip()
        if not raw: return []
        raw=raw.replace('\t','\n').replace(',','\n').replace('，','\n').replace(';','\n').replace('；','\n')
        vs=[v.strip() for v in raw.split('\n') if v.strip()]
        seen=set(); r=[]
        for v in vs:
            if v not in seen: seen.add(v); r.append(v)
        return r
    def _add(self):
        col=self.cmb_col.get(); opn=self.cmb_op.get()
        if not col or not opn: return
        oc=None; ot="single"
        for n,c,t in self.OPERATORS:
            if n==opn: oc=c; ot=t; break
        if ot=="single": val=self.e_val.get().strip(); dv=val
        elif ot=="multi":
            val=self._pmv()
            if not val: return
            dv=f"{len(val)}个值: {', '.join(val[:5])}{'...' if len(val)>5 else ''}"
        elif ot=="range":
            mn=self.e_min.get().strip(); mx=self.e_max.get().strip()
            if not mn or not mx: return
            val=(mn,mx); dv=f"{mn}~{mx}"
        else: val=None; dv=""
        expr=self._be(col,oc,val)
        if not expr: return
        self.conditions.append({'col':col,'op':opn,'val':dv,'expr':expr})
        self.tv.insert("","end",values=(col,opn,dv,expr)); self._upd()
        self.e_val.delete(0,tk.END); self.txt_m.delete("1.0",tk.END); self.e_min.delete(0,tk.END); self.e_max.delete(0,tk.END)
    def _be(self,col,oc,val):
        sc=f"`{col}`"
        if oc=="isna": return f"{sc}.isna()"
        if oc=="notna": return f"{sc}.notna()"
        if oc=="str.contains": return f"{sc}.str.contains('{val}',na=False)" if val else None
        if oc=="~str.contains": return f"~{sc}.str.contains('{val}',na=False)" if val else None
        if oc=="str.startswith": return f"{sc}.str.startswith('{val}')" if val else None
        if oc=="str.endswith": return f"{sc}.str.endswith('{val}')" if val else None
        if oc=="isin":
            if not val or not isinstance(val,list): return None
            items=[v if self._isnum(v) else f"'{v}'" for v in val]
            return f"{sc}.isin([{','.join(items)}])"
        if oc=="~isin":
            if not val or not isinstance(val,list): return None
            items=[v if self._isnum(v) else f"'{v}'" for v in val]
            return f"~{sc}.isin([{','.join(items)}])"
        if oc=="between":
            if not val or not isinstance(val,tuple): return None
            mn,mx=val
            if self._isnum(mn) and self._isnum(mx): return f"{sc}>={mn} and {sc}<={mx}"
            return f"{sc}>='{mn}' and {sc}<='{mx}'"
        if not val: return None
        return f"{sc} {oc} {val}" if self._isnum(val) else f"{sc} {oc} '{val}'"
    def _isnum(self,v):
        try: float(v); return True
        except: return False
    def _delc(self,e=None):
        sel=self.tv.selection()
        if not sel: return
        idx=self.tv.index(sel[0]); self.tv.delete(sel[0])
        if 0<=idx<len(self.conditions): self.conditions.pop(idx)
        self._upd()
    def _upd(self):
        self.e_res.delete(0,tk.END)
        if not self.conditions: return
        j=" and " if self.var_logic.get()=="AND" else " or "
        self.e_res.insert(0,j.join([c['expr'] for c in self.conditions]))
    def _clr(self):
        self.conditions=[]
        for i in self.tv.get_children(): self.tv.delete(i)
        self.e_res.delete(0,tk.END)
    def _ok(self):
        r=self.e_res.get().strip()
        if self.cb: self.cb(r)
        self.destroy()
