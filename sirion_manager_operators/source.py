# !/usr/bin/env python
# -*-coding:utf-8 -*-
# Author     ：Campanula 梦芸 何
import importlib
import traceback
from queue import SimpleQueue
from threading import Thread
from typing import Dict, Any, List, TypedDict, cast, Optional

from SirionDep.sirion_dep_frame.data_object_frame.data_object import DataContext
from SirionDep.sirion_dep_frame.plugin_frame import PluginBase
from SirionDep.sirion_dep_thread_manager.thread_pool_manager import t_pool_manager
from sirion_manager_logger.logger import error
from sirion_manager_operators.operator_base.base import OperatorBase
from sirion_manager_type_template.dag_defination import PluginParams



class SourceOperator(OperatorBase):
    def __init__(self, node_id: str, node_type: str, module_name: str, plugin_params: PluginParams,
                 operator_params: Dict[str, Any], global_config: Dict[str, Any], source_queue: List[SimpleQueue],
                 target_queue: List[SimpleQueue]) -> None:
        super().__init__(node_id, node_type, module_name, plugin_params, operator_params, global_config, source_queue,
                         target_queue)
        self.plugin:Optional[PluginBase] = None
        self._init_plugin()
        if len(self.target_queue) > 1:
            raise Exception(f"source类型算子之支持一个下游, 算子id {self.node_id}")
        t_pool_manager.add_task(task_name="source_read_loop",
                                task_function=self.read_data_from_source,
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


    def read_data_from_source(self):
        try:
            if self.plugin is None:
                return
            self.plugin.run([])
            data:Optional[List[DataContext]] = self.plugin.get_results()
            if data is not None:
                for queue in self.target_queue:
                    queue.put(data)
        except Exception as e:
            error(traceback.format_exc())
            error("source执行失败，失败原因-->",e,"插件模块名称:", self.module_name)






