# !/usr/bin/env python
# -*-coding:utf-8 -*-
# Author     ：Campanula 梦芸 何
from multiprocessing.queues import SimpleQueue
from typing import Dict, Any, List

from sirion_manager_type_template.dag_defination import PluginParams


class OperatorBase:
    def __init__(self, node_id: str, node_type: str, module_name: str,
                 plugin_params: PluginParams, operator_params: Dict[str, Any],
                 global_config: Dict[str, Any],
                 source_queue: List[SimpleQueue],
                 target_queue: List[SimpleQueue]) -> None:
        self.node_id: str = node_id
        self.node_type: str = node_type
        self.module_name: str = module_name
        self.plugin_params: PluginParams = plugin_params
        self.operator_params: Dict[str, Any] = operator_params
        self.global_config: Dict[str, Any] = global_config
        self.source_queue: List[SimpleQueue[Any]] = source_queue
        self.target_queue: List[SimpleQueue[Any]] = target_queue
