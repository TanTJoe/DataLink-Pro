"""主题与样式。"""
THEME = {
    "bg":"#F5F5F7","panel":"#FFFFFF","fg_primary":"#1D1D1F","fg_secondary":"#86868B",
    "accent":"#007AFF","accent_hover":"#0062CC","border":"#D2D2D7","input_bg":"#FFFFFF",
    "success":"#34C759","error":"#FF3B30","warning":"#FF9500","progress_fill":"#00C7FF",
    "font_main":("Segoe UI",9),"font_bold":("Segoe UI",9,"bold"),"font_large":("Segoe UI",11,"bold"),
    "font_mono":("Consolas",9),"font_mono_lg":("Consolas",10),
}

def apply_styles(style):
    style.theme_use('clam')
    style.configure(".",background=THEME['bg'],foreground=THEME['fg_primary'],font=THEME['font_main'])
    style.configure("TLabel",background=THEME['bg'],foreground=THEME['fg_primary'])
    style.configure("TLabelframe",background=THEME['bg'],bordercolor=THEME['border'],relief="solid",borderwidth=1)
    style.configure("TLabelframe.Label",background=THEME['bg'],foreground=THEME['accent'],font=THEME['font_bold'])
    style.configure("TButton",background="white",foreground=THEME['fg_primary'],bordercolor=THEME['border'],relief="solid",borderwidth=1)
    style.map("TButton",background=[("active","#E0E0E5"),("pressed","#D0D0D5")])
    style.configure("Accent.TButton",background=THEME['accent'],foreground="white",borderwidth=0)
    style.map("Accent.TButton",background=[("active","#0055BB"),("pressed","#004499")])
    style.configure("Success.TButton",background=THEME['success'],foreground="white",borderwidth=0,font=THEME['font_large'])
    style.map("Success.TButton",background=[("active","#2AA848"),("pressed","#228B3A")])
    style.configure("Stop.TButton",background=THEME['error'],foreground="white",borderwidth=0)
    style.map("Stop.TButton",background=[("active","#CC2222"),("disabled","#CCCCCC")])
    style.configure("TEntry",fieldbackground=THEME['input_bg'],bordercolor=THEME['border'],relief="solid")
    style.configure("Treeview",background="white",fieldbackground="white",bordercolor=THEME['border'],borderwidth=0)
    style.configure("Treeview.Heading",background="#F2F2F7",foreground=THEME['fg_primary'],relief="flat")
    style.map("Treeview",background=[("selected",THEME['accent'])],foreground=[("selected","white")])
    style.configure("TNotebook",background=THEME['bg'],borderwidth=0)
    style.configure("TNotebook.Tab",background=THEME['bg'],padding=[15,5],font=THEME['font_main'])
    style.map("TNotebook.Tab",background=[("selected","white")],foreground=[("selected",THEME['accent'])])
    style.configure("Indeterminate.Horizontal.TProgressbar",background=THEME['accent'])
    style.configure("Custom.Horizontal.TProgressbar",troughcolor=THEME['border'],bordercolor=THEME['border'],background=THEME['progress_fill'])
