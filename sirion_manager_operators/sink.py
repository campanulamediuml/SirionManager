# !/usr/bin/env python
# -*-coding:utf-8 -*-
# Author     ：Campanula 梦芸 何
import importlib
import traceback
from queue import SimpleQueue
from typing import Dict, Any, List, Optional

from SirionDep.sirion_dep_frame.data_object_frame.data_object import DataContext
from SirionDep.sirion_dep_frame.plugin_frame import PluginBase
from SirionDep.sirion_dep_thread_manager.thread_pool_manager import t_pool_manager
from sirion_manager_logger.logger import error
from sirion_manager_operators.operator_base.base import OperatorBase
from sirion_manager_type_template.dag_defination import PluginParams


class SinkOperator(OperatorBase):
    def __init__(self, node_id: str, node_type: str, module_name: str, plugin_params: PluginParams,
                 operator_params: Dict[str, Any], global_config: Dict[str, Any], source_queue: List[SimpleQueue],
                 target_queue: List[SimpleQueue]) -> None:
        super().__init__(node_id, node_type, module_name, plugin_params, operator_params, global_config, source_queue,
                         target_queue)
        self.plugin:Optional[PluginBase] = None
        if len(self.source_queue) > 1:
            raise Exception(f"sink类型算子不支持多个上游, 算子id {self.node_id}")
        if len(self.target_queue) > 0:
            raise Exception(f"sink类型算子不支持存在下游, 算子id {self.node_id}")
        self._init_plugin()
        t_pool_manager.add_task(task_name="sink_loop",
                                task_function=self._sink_loop,
                                is_interval=True)


    def _init_plugin(self):
        try:
            module = importlib.import_module(self.module_name)
            execute_instance:PluginBase = module.plugin(self.global_config)
            execute_instance.initial_work(self.plugin_params.get("parameter",{}))
            self.plugin = execute_instance
        except Exception as e:
            error(traceback.format_exc())
            error("算子节点id",self.node_id, "插件初始化失败 -->", e, "插件模块名称", self.module_name)

    def _sink_loop(self):
        try:
            data_ctx_list:List[DataContext] = self.source_queue[0].get()
            self.plugin.run(data_ctx_list)
        except Exception as e:
            error(traceback.format_exc())
            error(f"sink失败 {e}，算子节点id {self.node_id}")






