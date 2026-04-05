#!/usr/bin/env python3
"""数链通 DataLink Pro V50 入口。"""
import sys, os, traceback
_src = os.path.dirname(os.path.abspath(__file__))
if _src not in sys.path: sys.path.insert(0, _src)
from core.config import setup_logging, enable_dpi_awareness

def main():
    setup_logging(); enable_dpi_awareness()
    from ui.app import App; App().mainloop()

if __name__ == '__main__':
    try: main()
    except Exception as e:
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),'..','fatal.log'),'w',encoding='utf-8',errors='replace') as f:
            f.write(traceback.format_exc())
        try:
            from tkinter import messagebox; messagebox.showerror("Fatal",str(e))
        except: print(f"FATAL: {e}",file=sys.stderr)
        sys.exit(1)
