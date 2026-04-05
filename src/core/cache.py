"""Parquet 缓存系统。"""
import os, re, hashlib, logging
from typing import Tuple, Optional, Callable
from core.config import CACHE_DIR, ENABLE_CACHE, CACHE_VERSION, MASTER_MERGE_CACHE_DIR, ENABLE_MASTER_MERGE_CACHE, MASTER_MERGE_CACHE_VERSION

LogFunc = Optional[Callable[[str,str],None]]

def _quick_file_fingerprint(path,head_bytes=65536):
    st=os.stat(path); mtime_ns=getattr(st,"st_mtime_ns",int(st.st_mtime*1e9))
    h=hashlib.md5(); h.update(str(st.st_size).encode()); h.update(str(mtime_ns).encode())
    try:
        with open(path,"rb") as f:
            head=f.read(head_bytes); h.update(head)
            if st.st_size>head_bytes:
                try: f.seek(max(st.st_size-head_bytes,0)); h.update(f.read(head_bytes))
                except: pass
    except: pass
    return st, h.hexdigest()[:12]

def make_cache_key(abs_path,sheet,header_rows,usecols):
    st,fp=_quick_file_fingerprint(abs_path)
    mtime_ns=getattr(st,"st_mtime_ns",int(st.st_mtime*1e9))
    cols_key='ALL'
    if usecols:
        cols_key='UC'+str(len(usecols))+'_'+hashlib.md5('|'.join(map(str,usecols)).encode()).hexdigest()[:8]
    path_key=hashlib.md5(abs_path.encode()).hexdigest()[:10]
    f_id=f"{CACHE_VERSION}_{os.path.basename(abs_path)}_{path_key}_{mtime_ns}_{st.st_size}_{fp}_{sheet}_{header_rows}_{cols_key}"
    return re.sub(r'[\\/*?:"<>|]','_',f_id)

def get_cache_path(cache_key):
    os.makedirs(CACHE_DIR,exist_ok=True)
    return os.path.join(CACHE_DIR,cache_key+'.parquet')

def try_read_cache(cache_path,log_func=None):
    import polars as pl
    if not os.path.exists(cache_path): return None
    try:
        df=pl.read_parquet(cache_path)
        if log_func: log_func(f"[缓存命中] {os.path.basename(cache_path)}","info")
        return df
    except Exception as e:
        if log_func: log_func(f"[缓存读取失败] {e}","warn")
        return None

def write_cache(df,cache_path,log_func=None):
    try:
        df.write_parquet(cache_path)
        if log_func: log_func(f"[缓存写入] {os.path.basename(cache_path)}","info")
    except Exception as e:
        if log_func: log_func(f"[缓存写入失败] {e}","warn")

def make_master_merge_cache_key(folder,files,required_cols):
    if not ENABLE_MASTER_MERGE_CACHE: return None
    try:
        os.makedirs(MASTER_MERGE_CACHE_DIR,exist_ok=True)
        abs_folder=os.path.abspath(folder)
        path_key=hashlib.md5(abs_folder.encode()).hexdigest()[:10]
        cols_key=hashlib.md5('|'.join(required_cols).encode()).hexdigest()[:10] if required_cols else 'ALL'
        sig_parts=[]
        for f0 in files:
            try:
                st0,fp0=_quick_file_fingerprint(f0); mn=getattr(st0,'st_mtime_ns',int(st0.st_mtime*1e9))
                sig_parts.append(f"{os.path.basename(f0)}|{st0.st_size}|{mn}|{fp0}")
            except:
                st0=os.stat(f0); mn=getattr(st0,'st_mtime_ns',int(st0.st_mtime*1e9))
                sig_parts.append(f"{os.path.basename(f0)}|{st0.st_size}|{mn}")
        sig=hashlib.md5('\n'.join(sorted(sig_parts)).encode()).hexdigest()[:16]
        cache_id=f"{MASTER_MERGE_CACHE_VERSION}_MASTER_{path_key}_{cols_key}_{sig}_{len(files)}"
        return os.path.join(MASTER_MERGE_CACHE_DIR,re.sub(r'[\\/*?:"<>|]','_',cache_id)+'.parquet')
    except Exception as e:
        logging.warning(f"Master merge cache key failed: {e}")
        return None
