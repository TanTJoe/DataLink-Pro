"""V50 可复用UI组件：窗口不锁定、列名可搜索复制、自由切换层级。"""
import os, tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
from typing import List, Optional, Union
from ui.theme import THEME
from core.parser import idx_to_letter, letter_to_idx, split_smart

class AppleDialog(tk.Toplevel):
    """对话框基类。V50修复：transient保证层级关系，但不grab_set不锁焦点。"""

    def __init__(self, parent, title, modal=False):
        super().__init__(parent)
        self.title(title)
        self.configure(bg=THEME['bg'])
        self.protocol("WM_DELETE_WINDOW", self.destroy)
        # V50核心：transient保证此窗口始终在parent上面，但不锁定焦点
        self.transient(parent)
        self.lift()
        self.focus_force()
        if modal:
            self.grab_set()

class ColumnViewer(AppleDialog):
    """列名预览 — 可搜索、可复制列标/列名、不锁焦点。"""

    def __init__(self, parent, path, sheet=0, cols=None):
        sn = str(sheet) if isinstance(sheet, str) else f"Sheet {sheet}"
        super().__init__(parent, f"列名: {os.path.basename(path)} ({sn})")
        sw = min(500, int(self.winfo_screenwidth() * 0.4))
        sh = min(600, int(self.winfo_screenheight() * 0.7))
        self.geometry(f"{sw}x{sh}")
        self.all_cols = cols or []

        f = ttk.Frame(self, padding=10)
        f.pack(fill='both', expand=True)

        fs = ttk.Frame(f)
        fs.pack(fill='x', pady=(0, 5))
        ttk.Label(fs, text="搜索:").pack(side='left')
        self.var_s = tk.StringVar()
        self.var_s.trace_add("write", lambda *_: self._filter())
        e = ttk.Entry(fs, textvariable=self.var_s)
        e.pack(side='left', fill='x', expand=True, padx=5)
        e.focus()

        ttk.Label(f, text="选中文字 Ctrl+C 复制 → 粘贴到配置窗口",
                  foreground=THEME['fg_secondary']).pack(anchor='w')

        self.txt = scrolledtext.ScrolledText(f, font=THEME['font_mono_lg'],
                                             relief="flat", padx=10, pady=10)
        self.txt.pack(fill='both', expand=True)
        self._fill(self.all_cols)
        self.txt.bind("<Key>", self._on_key)

        fb = ttk.Frame(f)
        fb.pack(fill='x', pady=(5, 0))
        ttk.Label(fb, text=f"共{len(self.all_cols)}列",
                  foreground=THEME['fg_secondary']).pack(side='left')
        ttk.Button(fb, text="复制全部列标", command=self._copy_letters).pack(side='right', padx=3)
        ttk.Button(fb, text="复制全部列名", command=self._copy_names).pack(side='right', padx=3)
        ttk.Button(fb, text="关闭", command=self.destroy).pack(side='right', padx=3)

    def _fill(self, cols):
        self.txt.configure(state='normal')
        self.txt.delete('1.0', tk.END)
        self.txt.insert('1.0', "\n".join(
            [f"[{idx_to_letter(i)}] {c}" for i, c in enumerate(cols)]
        ))

    def _filter(self):
        key = self.var_s.get().strip().lower()
        if not key:
            self._fill(self.all_cols)
            return
        self.txt.configure(state='normal')
        self.txt.delete('1.0', tk.END)
        self.txt.insert('1.0', "\n".join(
            [f"[{idx_to_letter(i)}] {c}"
             for i, c in enumerate(self.all_cols) if key in str(c).lower()]
        ))

    def _on_key(self, e):
        if e.state & 0x4 and e.keysym.lower() in ('c', 'a'):
            return
        return "break"

    def _copy_letters(self):
        """复制全部列标：A+B+C+...+AE 格式"""
        result = "+".join([idx_to_letter(i) for i in range(len(self.all_cols))])
        self.clipboard_clear()
        self.clipboard_append(result)

    def _copy_names(self):
        """复制全部列名：一级物料号+一级物料名称+... 格式"""
        result = "+".join([str(c) for c in self.all_cols])
        self.clipboard_clear()
        self.clipboard_append(result)

class VirtualColumnMapDialog(AppleDialog):
    """主表列名映射 — 可搜索、可复制列标/列名、不锁焦点。"""

    def __init__(self, parent, mapping):
        super().__init__(parent, "主表列名 [虚拟列标]→字段名")
        self.geometry("520x600")
        self.all_lines = [(k, mapping[k]) for k in sorted(
            mapping.keys(), key=lambda x: letter_to_idx(x)
        )]

        f = ttk.Frame(self, padding=12)
        f.pack(fill='both', expand=True)

        fs = ttk.Frame(f)
        fs.pack(fill='x', pady=(0, 5))
        ttk.Label(fs, text="搜索:").pack(side='left')
        self.var_s = tk.StringVar()
        self.var_s.trace_add("write", lambda *_: self._filter())
        e = ttk.Entry(fs, textvariable=self.var_s)
        e.pack(side='left', fill='x', expand=True, padx=5)
        e.focus()

        ttk.Label(f, text="选中文字 Ctrl+C 复制 → 粘贴到配置窗口",
                  foreground=THEME['fg_secondary']).pack(anchor='w')

        self.txt = scrolledtext.ScrolledText(f, font=THEME['font_mono_lg'],
                                             relief="flat", padx=8, pady=8)
        self.txt.pack(fill='both', expand=True)
        self._fill(self.all_lines)
        self.txt.bind("<Key>", self._on_key)

        fb = ttk.Frame(f)
        fb.pack(fill='x', pady=(5, 0))
        ttk.Label(fb, text=f"共{len(self.all_lines)}列",
                  foreground=THEME['fg_secondary']).pack(side='left')
        ttk.Button(fb, text="复制全部列标", command=self._copy_letters).pack(side='right', padx=3)
        ttk.Button(fb, text="复制全部列名", command=self._copy_names).pack(side='right', padx=3)
        ttk.Button(fb, text="关闭", command=self.destroy).pack(side='right', padx=3)

    def _fill(self, lines):
        self.txt.configure(state='normal')
        self.txt.delete('1.0', tk.END)
        self.txt.insert('1.0', "\n".join(
            [f"[{k}]  {v}" for k, v in lines]
        ) if lines else "(空)")

    def _filter(self):
        key = self.var_s.get().strip().lower()
        if not key:
            self._fill(self.all_lines)
            return
        self._fill([(k, v) for k, v in self.all_lines
                     if key in v.lower() or key in k.lower()])

    def _on_key(self, e):
        if e.state & 0x4 and e.keysym.lower() in ('c', 'a'):
            return
        return "break"

    def _copy_letters(self):
        result = "+".join([k for k, v in self.all_lines])
        self.clipboard_clear()
        self.clipboard_append(result)

    def _copy_names(self):
        result = "+".join([v for k, v in self.all_lines])
        self.clipboard_clear()
        self.clipboard_append(result)

class SheetPickerDialog(AppleDialog):
    def __init__(self,master,sheet_names,callback):
        super().__init__(master,"工作表选择",modal=True)
        self.geometry("450x500"); self.cb=callback; self.names=sheet_names; self.filtered=list(sheet_names); self._ui()
    def _ui(self):
        f=ttk.Frame(self,padding=15); f.pack(fill='both',expand=True)
        fs=ttk.Frame(f); fs.pack(fill='x')
        ttk.Label(fs,text="搜索:").pack(side='left')
        self.var_s=tk.StringVar(); self.var_s.trace("w",self._filt)
        e=ttk.Entry(fs,textvariable=self.var_s); e.pack(side='left',fill='x',expand=True,padx=5); e.focus()
        self.lb=tk.Listbox(f,selectmode='single',height=8,relief="flat",bg=THEME['input_bg'],selectbackground=THEME['accent'],selectforeground='white')
        self.lb.pack(fill='both',expand=True,pady=10); self.lb.bind("<Double-1>",self._ok); self._ref()
        ttk.Button(f,text="确认",style="Accent.TButton",command=self._ok).pack(fill='x')
    def _filt(self,*a):
        q=self.var_s.get().lower(); self.filtered=[s for s in self.names if q in s.lower()]; self._ref()
    def _ref(self):
        self.lb.delete(0,tk.END)
        for s in self.filtered: self.lb.insert(tk.END,s)
    def _ok(self,e=None):
        sel=self.lb.curselection()
        if sel: self.cb(self.filtered[sel[0]]); self.destroy()

class SheetMultiPickerDialog(AppleDialog):
    def __init__(self,master,path,sheet_names,callback):
        super().__init__(master,f"工作表选择: {os.path.basename(path)}",modal=True)
        self.geometry("450x500"); self.cb=callback; self.names=sheet_names; self._ui()
    def _ui(self):
        f=ttk.Frame(self,padding=15); f.pack(fill='both',expand=True)
        ttk.Label(f,text="单选=仅使用该表 | 多选=纵向合并",font=THEME['font_bold']).pack(fill='x',pady=5)
        self.lb=tk.Listbox(f,selectmode='extended',height=8,relief="flat",bg=THEME['input_bg'],selectbackground=THEME['accent'],selectforeground='white')
        self.lb.pack(fill='both',expand=True,pady=10)
        for i,s in enumerate(self.names): self.lb.insert(tk.END,f"[{i}] {s}")
        self.lb.bind("<Double-1>",self._ok)
        ttk.Button(f,text="确认",style="Accent.TButton",command=self._ok).pack(fill='x',pady=10)
    def _ok(self,e=None):
        sels=self.lb.curselection()
        if not sels: messagebox.showwarning("提示","请选择至少一个"); return
        sel=[self.names[i] for i in sels]
        self.cb(sel[0] if len(sel)==1 else sel); self.destroy()

class MasterSchemaDesignerDialog(AppleDialog):
    def __init__(self,parent,all_cols,selected_cols=None,callback=None):
        super().__init__(parent,"主表结构配置",modal=True)
        self.all_cols=list(all_cols or []); self.selected=list(selected_cols or []); self.callback=callback
        sw=min(1000,int(self.winfo_screenwidth()*0.8)); sh=min(600,int(self.winfo_screenheight()*0.8))
        self.geometry(f"{sw}x{sh}"); self._ui()
    def _ui(self):
        root=ttk.Frame(self,padding=15); root.pack(fill='both',expand=True)
        ttk.Label(root,text="左侧选源列，右侧为保留列（顺序=虚拟列标顺序）",foreground=THEME['fg_secondary']).pack(anchor='w',pady=(0,10))
        paned=ttk.PanedWindow(root,orient='horizontal'); paned.pack(fill='both',expand=True)
        lf=ttk.Frame(paned,padding=(0,0,10,0)); paned.add(lf,weight=1)
        ttk.Label(lf,text="所有源列").pack(anchor='w')
        fs=ttk.Frame(lf); fs.pack(fill='x',pady=5)
        ttk.Label(fs,text="搜索:").pack(side='left')
        self.var_s=tk.StringVar(); ttk.Entry(fs,textvariable=self.var_s).pack(side='left',fill='x',expand=True,padx=5)
        self.var_s.trace_add("write",lambda *_:self._rl())
        self.lb_l=tk.Listbox(lf,selectmode='extended',relief="flat",bg=THEME['input_bg'],exportselection=False,selectbackground=THEME['accent'],selectforeground='white')
        self.lb_l.pack(fill='both',expand=True); self.lb_l.bind('<Double-1>',lambda e:self._add())
        mid=ttk.Frame(paned,padding=(5,0,5,0)); paned.add(mid,weight=0)
        for t,c in [("添加>",self._add),("移除<",self._rem),("全部>>",self._addall),("清空<<",self._clr)]:
            ttk.Button(mid,text=t,command=c).pack(fill='x',pady=3)
        rf=ttk.Frame(paned,padding=(10,0,0,0)); paned.add(rf,weight=1)
        ttk.Label(rf,text="保留列（顺序=虚拟列标）").pack(anchor='w')
        self.lb_r=tk.Listbox(rf,selectmode='extended',relief="flat",bg=THEME['input_bg'],exportselection=False,selectbackground=THEME['accent'],selectforeground='white')
        self.lb_r.pack(fill='both',expand=True)
        fo=ttk.Frame(rf); fo.pack(fill='x',pady=5)
        for t,c in [("▲",lambda:self._mv(-1)),("▼",lambda:self._mv(1)),("⤒",self._mt),("⤓",self._mb)]:
            ttk.Button(fo,text=t,command=c,width=3).pack(side='left',padx=2)
        ttk.Frame(fo).pack(side='left',fill='x',expand=True)
        ttk.Button(fo,text="保存",style="Accent.TButton",command=self._ok).pack(side='right')
        self._rl(); self._rr()
    def _fa(self):
        key=(self.var_s.get() or "").strip().lower(); base=[c for c in self.all_cols if c not in self.selected]
        return [c for c in base if key in str(c).lower()] if key else base
    def _rl(self):
        self.lb_l.delete(0,tk.END)
        for c in self._fa(): self.lb_l.insert(tk.END,c)
    def _rr(self):
        self.lb_r.delete(0,tk.END)
        for i,c in enumerate(self.selected): self.lb_r.insert(tk.END,f"[{idx_to_letter(i)}] {c}")
    def _add(self):
        for i in self.lb_l.curselection():
            c=self.lb_l.get(i)
            if c not in self.selected: self.selected.append(c)
        self._rl(); self._rr()
    def _addall(self):
        for c in self._fa():
            if c not in self.selected: self.selected.append(c)
        self._rl(); self._rr()
    def _rem(self):
        for i in sorted(self.lb_r.curselection(),reverse=True):
            if 0<=i<len(self.selected): self.selected.pop(i)
        self._rl(); self._rr()
    def _clr(self): self.selected=[]; self._rl(); self._rr()
    def _mv(self,d):
        idxs=list(self.lb_r.curselection())
        if len(idxs)!=1: return
        i=idxs[0]; j=i+d
        if j<0 or j>=len(self.selected): return
        self.selected[i],self.selected[j]=self.selected[j],self.selected[i]; self._rr(); self.lb_r.selection_set(j)
    def _mt(self):
        idxs=sorted(self.lb_r.curselection()); picked=[self.selected[i] for i in idxs if 0<=i<len(self.selected)]
        if not picked: return
        rest=[c for k,c in enumerate(self.selected) if k not in set(idxs)]; self.selected=picked+rest; self._rr()
    def _mb(self):
        idxs=sorted(self.lb_r.curselection()); picked=[self.selected[i] for i in idxs if 0<=i<len(self.selected)]
        if not picked: return
        rest=[c for k,c in enumerate(self.selected) if k not in set(idxs)]; self.selected=rest+picked; self._rr()
    def _ok(self):
        if not self.selected:
            if messagebox.askyesno("提示","未选列，使用全部列？"):
                if self.callback: self.callback(None)
                self.destroy()
            return
        if self.callback: self.callback(self.selected)
        self.destroy()

class MultiSortDialog(AppleDialog):
    def __init__(self,parent,all_cols,current_sorts):
        super().__init__(parent,"多重排序配置",modal=True)
        self.geometry("450x500"); self.all_cols=all_cols; self.result=None; self._ui(current_sorts)
    def _ui(self,cs):
        f=ttk.Frame(self,padding=10); f.pack(fill='both',expand=True)
        ttk.Label(f,text="排序字段（越靠前优先级越高）:").pack(anchor='w',pady=5)
        self.tv=ttk.Treeview(f,columns=("字段","顺序"),show='headings',height=8)
        self.tv.heading("字段",text="字段名"); self.tv.column("字段",width=250)
        self.tv.heading("顺序",text="排序方式（双击切换）"); self.tv.column("顺序",width=150)
        self.tv.pack(fill='both',expand=True,pady=5); self.tv.bind("<Double-1>",self._toggle)
        for item in cs:
            if item['col'] in self.all_cols: self.tv.insert("","end",values=(item['col'],"升序" if item['asc'] else "降序"))
        fa=ttk.Frame(f); fa.pack(fill='x',pady=5)
        self.cmb=ttk.Combobox(fa,values=self.all_cols,state='readonly',width=30); self.cmb.pack(side='left',padx=5)
        ttk.Button(fa,text="添加",command=self._add).pack(side='left')
        ttk.Button(fa,text="删除选中",command=self._del).pack(side='right')
        ttk.Button(f,text="确认",command=self._save,style="Accent.TButton").pack(fill='x',pady=10)
    def _add(self):
        v=self.cmb.get()
        if v: self.tv.insert("","end",values=(v,"升序"))
    def _del(self):
        for i in self.tv.selection(): self.tv.delete(i)
    def _toggle(self,e):
        item=self.tv.identify_row(e.y)
        if not item: return
        vals=list(self.tv.item(item,"values")); vals[1]="降序" if "升序" in vals[1] else "升序"
        self.tv.item(item,values=vals)
    def _save(self):
        self.result=[{'col':self.tv.item(i,"values")[0],'asc':"升序" in self.tv.item(i,"values")[1]} for i in self.tv.get_children()]
        self.destroy()

class SmartColumnPicker(AppleDialog):
    def __init__(self,parent,all_cols,callback,multi=False):
        super().__init__(parent,"选择数据列",modal=True)
        self.geometry("450x500"); self.all_cols=all_cols; self.cb=callback; self.multi=multi; self.filtered=list(all_cols); self._ui()
    def _ui(self):
        f=ttk.Frame(self,padding=10); f.pack(fill='both',expand=True)
        fs=ttk.Frame(f); fs.pack(fill='x')
        ttk.Label(fs,text="搜索:").pack(side='left')
        self.var_s=tk.StringVar(); self.var_s.trace("w",self._filt)
        self.es=ttk.Entry(fs,textvariable=self.var_s); self.es.pack(side='left',fill='x',expand=True,padx=5); self.es.focus()
        mode='extended' if self.multi else 'single'
        self.lb=tk.Listbox(f,selectmode=mode,height=15,relief="flat",bg=THEME['input_bg'],selectbackground=THEME['accent'],selectforeground='white')
        self.lb.pack(fill='both',expand=True,pady=10); self.lb.bind("<Double-1>",self._ok); self._ref()
        ttk.Button(f,text="确认",style="Accent.TButton",command=self._ok).pack(fill='x',pady=5)
    def _filt(self,*a):
        q=self.var_s.get().lower(); self.filtered=[c for c in self.all_cols if q in str(c).lower()]; self._ref()
    def _ref(self):
        self.lb.delete(0,tk.END)
        for c in self.filtered: self.lb.insert(tk.END,c)
    def _ok(self,e=None):
        sels=self.lb.curselection()
        if not sels: return
        res=[self.filtered[i] for i in sels]; self.cb(res if self.multi else res[0]); self.destroy()
