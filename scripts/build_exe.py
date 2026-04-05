"""PyInstaller 打包脚本。用法: python scripts/build_exe.py"""
import os, sys, subprocess
ROOT=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC=os.path.join(ROOT,'src'); MAIN=os.path.join(SRC,'main.py'); ICON=os.path.join(ROOT,'assets','icon.ico')
def build():
    cmd=[sys.executable,'-m','PyInstaller','--onefile','--windowed','--name','DataLinkPro',
         '--distpath',os.path.join(ROOT,'dist'),'--workpath',os.path.join(ROOT,'build'),
         '--specpath',os.path.join(ROOT,'build'),'--paths',SRC,
         '--hidden-import','polars','--hidden-import','xlsxwriter','--hidden-import','openpyxl',
         '--hidden-import','chardet','--hidden-import','pyarrow','--hidden-import','fastexcel']
    if os.path.exists(ICON): cmd.extend(['--icon',ICON])
    cmd.append(MAIN)
    print(f"Building..."); r=subprocess.run(cmd,cwd=ROOT)
    if r.returncode==0:
        exe=os.path.join(ROOT,'dist','DataLinkPro.exe')
        print(f"\n打包成功: {exe}\n大小: {os.path.getsize(exe)/1024/1024:.1f}MB")
    else: print(f"\n打包失败: {r.returncode}"); sys.exit(1)
if __name__=='__main__': build()
