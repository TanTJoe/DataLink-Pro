"""全局配置与常量。"""
import os, sys, logging, warnings

APP_NAME = "数链通 DataLink Pro"
APP_VERSION = "V50"
APP_TITLE = f"{APP_NAME} {APP_VERSION}"

def _get_app_dir() -> str:
    try:
        if getattr(sys, 'frozen', False):
            return os.path.dirname(sys.executable)
    except Exception:
        pass
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

APP_DIR = _get_app_dir()
LOG_FILE = os.path.join(APP_DIR, f'datalink_{APP_VERSION.lower()}_error.log')
PRESETS_FILE = os.path.join(APP_DIR, f'DataLink_{APP_VERSION}_Presets.json')
AUTOSAVE_FILE = os.path.join(APP_DIR, 'DataLink_AutoSave.json')
CONFIG_FILE = os.path.join(APP_DIR, 'DataLink_Config.json')
CACHE_DIR = os.path.join(APP_DIR, 'DataLink_Cache_Parquet')
ENABLE_CACHE = True
CACHE_VERSION = 'v3'
MASTER_MERGE_CACHE_DIR = os.path.join(APP_DIR, 'DataLink_Cache_MasterMerge')
ENABLE_MASTER_MERGE_CACHE = True
MASTER_MERGE_CACHE_VERSION = 'v2'

try:
    import xlsxwriter; XLSX_ENGINE = 'xlsxwriter'
except ImportError:
    XLSX_ENGINE = 'openpyxl'

warnings.filterwarnings('ignore')

def setup_logging():
    _fh = logging.FileHandler(LOG_FILE, encoding='utf-8', errors='replace')
    logging.basicConfig(level=logging.ERROR, handlers=[_fh], format='%(asctime)s %(levelname)s: %(message)s')

def enable_dpi_awareness():
    try:
        from ctypes import windll; windll.shcore.SetProcessDpiAwareness(1)
    except Exception: pass
