# !/usr/bin/env python
# -*-coding:utf-8 -*-
# Author     ：Campanula 梦芸 何
from typing import TypedDict, Dict, List, Any, LiteralString, Literal

TypeOperatorType = Literal[
    'source',
    'sink',
    'merge',
    'split',
    'transform'
]


class PluginParams(TypedDict):
    common_param: Dict[str,Any]
    parameter: Dict[str, Dict]


class TypeDAGNode(TypedDict):
    node_id: str
    node_type: str
    module_name: str
    operator_type: TypeOperatorType
    operator_params: Dict
    plugin_params: PluginParams

class TypeDAGEdge(TypedDict):
    edge_id: str
    source_node: str
    target_node: str

class DAGConfig(TypedDict):
    global_config: Dict[str, Any]
    nodes:List[TypeDAGNode]
    edges:List[TypeDAGEdge]

