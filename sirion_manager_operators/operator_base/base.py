# !/usr/bin/env python
# -*-coding:utf-8 -*-
# Author     ：Campanula 梦芸 何
import importlib
from queue import SimpleQueue
from typing import Dict, Any, List, Optional

from pydantic.mypy import plugin

from SirionDep.sirion_dep_frame.data_object_frame.data_object import DataContext
from SirionDep.sirion_dep_frame.plugin_frame import PluginBase
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
        self.source_queue: List[SimpleQueue[List[DataContext]]]= source_queue
        self.target_queue: List[SimpleQueue[List[DataContext]]] = target_queue
        self.common_params = self.plugin_params.get("common_param", {})


    def _init_plugin(self):
        """
        初始化实例池
        :return:
        """
        return


    def _load_common_param(self):
        """
        自动分配通用参数
        :return:
        """
        for key in self.plugin_params['parameter']:
            self.plugin_params['parameter'][key].update(self.common_params)



