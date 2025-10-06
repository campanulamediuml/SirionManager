# !/usr/bin/env python
# -*-coding:utf-8 -*-
# Author     ：Campanula 梦芸 何
import importlib
import traceback
from queue import SimpleQueue
from threading import Thread
from typing import Dict, Any, List, TypedDict, cast, Optional

from SirionDep.sirion_dep_frame.data_object_frame.data_object import DataContext
from SirionDep.sirion_dep_frame.plugin_frame import PuginBase
from sirion_manager_logger.logger import error
from sirion_manager_operators.operator_base.base import OperatorBase
from sirion_manager_type_template.dag_defination import PluginParams



class SourceOperator(OperatorBase):
    def __init__(self, node_id: str, node_type: str, module_name: str, plugin_params: PluginParams,
                 operator_params: Dict[str, Any], global_config: Dict[str, Any], source_queue: List[SimpleQueue],
                 target_queue: List[SimpleQueue]) -> None:
        super().__init__(node_id, node_type, module_name, plugin_params, operator_params, global_config, source_queue,
                         target_queue)
        self.plugin:Optional[PuginBase] = None
        self._init_plugin()
        self.source_task = Thread(target=self.source_read_task, daemon=True)
        self.source_task.start()

    def _init_plugin(self):
        module = importlib.import_module(self.module_name)
        execute_instance:PuginBase = module.plugin(self.global_config)
        execute_instance.initial_work(self.plugin_params.get("parameter",{}))
        self.plugin = execute_instance

    def source_read_task(self):
        while True:
            try:
                self.read_data_from_source()
            except Exception as e:
                err_info = traceback.format_exc()
                error(err_info)
                error("source执行出错 ---> ",e)

    def read_data_from_source(self):
        if self.plugin is None:
            return
        self.plugin.run([])
        data:Optional[List[DataContext]] = self.plugin.get_results()
        if data is not None:
            for queue in self.target_queue:
                queue.put(data)






