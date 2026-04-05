"""数据模型。"""
from dataclasses import dataclass, asdict, field
from typing import List, Dict, Optional, Union
from core.parser import resolve_cols_expr

@dataclass
class MatchRule:
    target_path: str; sheet_idx: Union[int,str]; by_letter: bool
    master_keys: str; target_keys: str; pick_cols: str; col_prefix: str
    _m_cols: List[str]=field(default_factory=list)
    _t_cols: List[str]=field(default_factory=list)
    _p_cols: List[str]=field(default_factory=list)
    @property
    def out_prefix(self): return self.col_prefix
    @out_prefix.setter
    def out_prefix(self,v): self.col_prefix=v
    def parse(self,m_cols,t_cols):
        self._m_cols=resolve_cols_expr(self.master_keys,m_cols,self.by_letter,"主表关联列")
        self._t_cols=resolve_cols_expr(self.target_keys,t_cols,self.by_letter,"目标关联列")
        self._p_cols=resolve_cols_expr(self.pick_cols,t_cols,self.by_letter,"目标取回列")
        if len(self._m_cols)!=len(self._t_cols):
            raise ValueError(f"关联列数不一致：{len(self._m_cols)} vs {len(self._t_cols)}")

@dataclass
class PivotConfig:
    enabled: bool=False; group_by: List[str]=field(default_factory=list)
    pivot_cols: List[str]=field(default_factory=list)
    values_agg: Dict[str,str]=field(default_factory=dict); renames: str=""

@dataclass
class OutputTask:
    name: str; filter_expr: str; calc_fields: str=""
    sort_config: List[Dict]=field(default_factory=list)
    col_map: List[Dict[str,str]]=field(default_factory=list)
    pivot_cfg: PivotConfig=field(default_factory=PivotConfig)
    @classmethod
    def from_dict(cls,data):
        if 'pivot_cfg' in data and isinstance(data['pivot_cfg'],dict):
            dp=PivotConfig()
            pc={k:data['pivot_cfg'].get(k,getattr(dp,k)) for k in ('enabled','group_by','pivot_cols','values_agg','renames')}
            pc['group_by']=list(pc['group_by']); pc['pivot_cols']=list(pc['pivot_cols']); pc['values_agg']=dict(pc['values_agg'])
            data['pivot_cfg']=PivotConfig(**pc)
        return cls(**data)

def task_to_dict(t): return asdict(t)
def rule_to_dict(r):
    d=asdict(r); d.pop('_m_cols',None); d.pop('_t_cols',None); d.pop('_p_cols',None); return d
