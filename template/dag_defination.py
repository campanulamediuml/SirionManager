# !/usr/bin/env python
# -*-coding:utf-8 -*-
# Author     ：Campanula 梦芸 何
from typing import TypedDict, Dict, List


class PluginParams(TypedDict):
    common_param: Dict
    parameter: Dict[str, Dict]


class TypeDAGNode(TypedDict):
    id: str
    node_type: str
    module_name: str
    operator_type: str
    operator_params: Dict
    plugin_params: PluginParams

class TypeDAGEdge(TypedDict):
    id: str
    source_node: str
    target_node: str

class DAGConfig(TypedDict):
    global_config: Dict[str, Dict]
    nodes:List[TypeDAGNode]
    edges:List[TypeDAGEdge]

