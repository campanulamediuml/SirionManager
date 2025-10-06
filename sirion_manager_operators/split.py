# !/usr/bin/env python
# -*-coding:utf-8 -*-
# Author     ：Campanula 梦芸 何
import traceback
from copy import deepcopy
from queue import SimpleQueue
from typing import Dict, Any, List

from SirionDep.sirion_dep_frame.data_object_frame.data_object import DataContext
from SirionDep.sirion_dep_thread_manager.thread_pool_manager import t_pool_manager
from sirion_manager_logger.logger import error
from sirion_manager_operators.operator_base.base import OperatorBase
from sirion_manager_type_template.dag_defination import PluginParams


class SplitOperator(OperatorBase):
    def __init__(self, node_id: str, node_type: str, module_name: str, plugin_params: PluginParams,
                 operator_params: Dict[str, Any], global_config: Dict[str, Any], source_queue: List[SimpleQueue],
                 target_queue: List[SimpleQueue]) -> None:
        super().__init__(node_id, node_type, module_name, plugin_params, operator_params, global_config, source_queue,
                         target_queue)
        if len(self.source_queue) < 1:
            raise Exception(f"算子id{self,node_id}缺少上游")
        if len(self.source_queue) > 1:
            raise Exception(f"算子id{self,node_id}不是merge类型，上游只支持一个算子")
        if len(self.target_queue) <= 1:
            raise Exception("split类型算子需要通往至少两个下游队列")
        t_pool_manager.add_task(task_name="source_read_loop",
                                task_function=self.split_task,
                                is_interval=True)


    def split_task(self):
        try:
            data:List[DataContext] = self.source_queue[0].get()
            for next_queue in self.target_queue:
                next_queue.put(deepcopy(data))
        except Exception as e:
            error(traceback.format_exc())
            error("执行分流操作失败-->",e)