"""工具函数。"""
import os, json, logging
from typing import Optional
from core.config import PRESETS_FILE, CONFIG_FILE

def unique_path(path):
    base,ext=os.path.splitext(path); c=1
    while os.path.exists(path): path=f"{base}_{c}{ext}"; c+=1
    return path

def load_presets():
    if os.path.exists(PRESETS_FILE):
        try:
            with open(PRESETS_FILE,'r',encoding='utf-8') as f: return json.load(f)
        except: return {}
    return {}

def save_presets(data):
    try:
        with open(PRESETS_FILE,'w',encoding='utf-8') as f: json.dump(data,f,ensure_ascii=False,indent=2)
    except Exception as e: logging.error(f"Save preset: {e}")

def save_last_active(name):
    try:
        with open(CONFIG_FILE,'w',encoding='utf-8') as f: json.dump({"last_active":name},f)
    except: pass

def load_last_active():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE,'r',encoding='utf-8') as f: return json.load(f).get("last_active")
        except: pass
    return None
