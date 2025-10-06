# !/usr/bin/env python
# -*-coding:utf-8 -*-
# Author     ：Campanula 梦芸 何
import importlib
import traceback
from queue import SimpleQueue
from typing import Dict, Any, List

from SirionDep.sirion_dep_frame.plugin_frame import PluginBase
from sirion_manager_logger.logger import error
from sirion_manager_operators.operator_base.base import OperatorBase
from sirion_manager_type_template.dag_defination import PluginParams


class TransformOperator(OperatorBase):
    def __init__(self, node_id: str, node_type: str, module_name: str, plugin_params: PluginParams,
                 operator_params: Dict[str, Any], global_config: Dict[str, Any], source_queue: List[SimpleQueue],
                 target_queue: List[SimpleQueue]) -> None:
        super().__init__(node_id, node_type, module_name, plugin_params, operator_params, global_config, source_queue,
                         target_queue)
        if len(self.source_queue) != 1:
            raise Exception("Transform类型算子上游数量不是1，错误")
        if len(self.target_queue) != 0:
            raise Exception("Transform类型算子下游数量不是1，错误")
        self._load_common_param()
        self.parameters:Dict[str,Dict] = self.plugin_params['parameter']
        self.plugin_collections:Dict[str,PluginBase] = {}
        self._init_plugin()

    def _init_plugin(self):
        try:
            module = importlib.import_module(self.module_name)
            for data_tag, parameter in self.parameters.items():
                execute_instance:PluginBase = module.plugin(self.global_config)
                execute_instance.initial_work(parameter)
                self.plugin_collections[data_tag] = execute_instance
        except Exception as e:
            error(traceback.format_exc())
            error("算子节点id",self.node_id, "插件初始化失败 -->", e, "插件模块名称", self.module_name)





if __name__ == '__main__':
    print(TransformOperator)
