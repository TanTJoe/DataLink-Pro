"""应用主窗口。"""
import os, sys, ctypes, tkinter as tk
from tkinter import ttk
from core.config import APP_TITLE
from ui.theme import THEME, apply_styles
from ui.main_frame import AdvancedReportFrame

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("1400x1000")
        self.configure(bg=THEME['bg'])
        try: ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("datalink.pro.v50")
        except: pass
        self._set_icon()
        apply_styles(ttk.Style())
        AdvancedReportFrame(self).pack(fill='both',expand=True)

    def _set_icon(self):
        try:
            base=os.path.dirname(sys.executable) if getattr(sys,'frozen',False) else os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            ico=os.path.join(base,'assets','icon.ico')
            if os.path.exists(ico): self.iconbitmap(ico)
        except: pass
