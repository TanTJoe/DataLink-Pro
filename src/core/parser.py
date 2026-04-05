"""列名解析器。"""
import re
from typing import List, Optional

def idx_to_letter(idx: int) -> str:
    idx += 1; s = ''
    while idx: idx, rem = divmod(idx - 1, 26); s = chr(65 + rem) + s
    return s

def letter_to_idx(letter: str) -> int:
    idx = 0
    for ch in letter.upper():
        if 'A' <= ch <= 'Z': idx = idx * 26 + ord(ch) - 64
    return idx - 1

LETTER_RE = re.compile(r'^\s*\[?\s*([A-Za-z]{1,3})\s*\]?\s*$')
RANGE_RE = re.compile(r'^\s*\[?\s*([A-Za-z]{1,3})\s*\]?\s*:\s*\[?\s*([A-Za-z]{1,3})\s*\]?\s*$')

def split_smart(text: str) -> List[str]:
    if not text: return []
    text = text.replace('\uff1b',',').replace(';',',').replace('\n',',').replace('\uff0c',',')
    text = text.replace('\u2018',"'").replace('\u2019',"'").replace('\u201c','"').replace('\u201d','"')
    result=[]; buffer=""; paren=0; in_q=False; qc=''
    for ch in text:
        if ch in ['"',"'"]:
            if not in_q: in_q=True; qc=ch
            elif ch==qc: in_q=False
        if not in_q:
            if ch=='(': paren+=1
            elif ch==')': paren-=1
        if ch==',' and paren==0 and not in_q:
            if buffer.strip(): result.append(buffer.strip())
            buffer=""
        else: buffer+=ch
    if buffer.strip(): result.append(buffer.strip())
    return result

def split_plus(expr: str) -> List[str]:
    return [s.strip() for s in expr.replace(',','+').split('+') if s.strip()]

def parse_letter_range(expr: str) -> List[int]:
    out=[]
    for part in expr.replace(',','+').split('+'):
        part=part.strip()
        if not part: continue
        if ':' in part:
            try:
                a,b=part.split(':'); s,e=letter_to_idx(a),letter_to_idx(b)
                out.extend(range(min(s,e),max(s,e)+1))
            except: pass
        else: out.append(letter_to_idx(part))
    return out

def _normalize_name_token(tok):
    tok=str(tok).strip()
    if tok.startswith('"') and tok.endswith('"'): tok=tok[1:-1].strip()
    if tok.startswith("'") and tok.endswith("'"): tok=tok[1:-1].strip()
    return tok

def _try_match_name(tok,cols):
    tok=_normalize_name_token(tok)
    if tok in cols: return tok
    low=tok.lower(); cand=[c for c in cols if str(c).lower()==low]
    return cand[0] if len(cand)==1 else None

def _try_parse_letter(tok,cols):
    tok=_normalize_name_token(tok); m=LETTER_RE.match(tok)
    if not m: return None
    idx=letter_to_idx(m.group(1))
    return cols[idx] if 0<=idx<len(cols) else None

def _expand_range(tok,cols):
    tok=_normalize_name_token(tok); m=RANGE_RE.match(tok)
    if not m: return []
    s,e=letter_to_idx(m.group(1)),letter_to_idx(m.group(2))
    if s<0 or e<0: return []
    a,b=min(s,e),max(s,e)
    return [cols[i] for i in range(a,b+1) if 0<=i<len(cols)]

def resolve_cols_expr(expr,cols,prefer_by_letter,ctx=""):
    if expr is None: return []
    parts=split_plus(str(expr)); out=[]
    for raw in parts:
        tok=raw.strip()
        if not tok: continue
        r=_expand_range(tok,cols)
        if r: out.extend(r); continue
        if prefer_by_letter:
            c=_try_parse_letter(tok,cols)
            if c: out.append(c); continue
            c=_try_match_name(tok,cols)
            if c: out.append(c); continue
        else:
            c=_try_match_name(tok,cols)
            if c: out.append(c); continue
            c=_try_parse_letter(tok,cols)
            if c: out.append(c); continue
        raise ValueError(f"{ctx}解析失败：'{tok}' 不存在。\n可用列：{cols[:30]}{'...' if len(cols)>30 else ''}")
    seen=set(); final=[]
    for c in out:
        if c not in seen: seen.add(c); final.append(c)
    return final
