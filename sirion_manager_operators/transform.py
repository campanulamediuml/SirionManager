# !/usr/bin/env python
# -*-coding:utf-8 -*-
# Author     ：Campanula 梦芸 何
import importlib
import traceback
from queue import SimpleQueue
from typing import Dict, Any, List

from SirionDep.sirion_dep_frame.data_object_frame.data_object import DataContext
from SirionDep.sirion_dep_frame.plugin_frame import PluginBase
from SirionDep.sirion_dep_thread_manager.thread_pool_manager import t_pool_manager
from sirion_manager_logger.logger import error, info
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
        t_pool_manager.add_task(task_name="transform_task",
                                task_function=self._transfrom_run,
                                is_interval=True)

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

    def _transfrom_run(self):
        """
        transform类型算子运行
        :return:
        """
        data_set:List[DataContext] = self.source_queue[0].get()
        for data_ctx in data_set:
            self.__execute(data_ctx)


    def __execute(self, data_ctx:DataContext):
        try:
            data_tag = data_ctx.get_data_tag()
            plugin = self.plugin_collections.get(data_tag)
            if plugin is None:
                 return info("一条数据没有对应插件实例-->", data_tag)
            plugin.run([data_ctx])
            res = plugin.get_results()
            self.target_queue[0].put(res)
        except Exception as e:
            error(traceback.format_exc())
            error(f"transform类型算子执行错误, 算子节点id {self.node_id}, 数据内容:", data_ctx.get_data())











if __name__ == '__main__':
    print(TransformOperator)
