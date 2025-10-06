# !/usr/bin/env python
# -*-coding:utf-8 -*-
# Author     ：Campanula 梦芸 何
import importlib
import traceback
from queue import SimpleQueue
from typing import Dict, Any, List, Tuple

from SirionDep.sirion_dep_frame.data_object_frame.data_object import DataContext
from SirionDep.sirion_dep_frame.plugin_frame import PluginBase
from SirionDep.sirion_dep_thread_manager.thread_pool_manager import t_pool_manager
from sirion_manager_logger.logger import error, warn
from sirion_manager_operators.operator_base.base import OperatorBase
from sirion_manager_type_template.dag_defination import PluginParams




class MergeOperator(OperatorBase):
    def __init__(self, node_id: str, node_type: str, module_name: str, plugin_params: PluginParams,
                 operator_params: Dict[str, Any], global_config: Dict[str, Any], source_queue: List[SimpleQueue],
                 target_queue: List[SimpleQueue]) -> None:
        super().__init__(node_id, node_type, module_name, plugin_params, operator_params, global_config, source_queue,
                         target_queue)
        self._load_common_param()
        self.parameters:Dict[str,Dict] = self.plugin_params['parameter']
        self.plugin_collections:Dict[str,PluginBase] = {}
        self.before_merge_queue:SimpleQueue[DataContext] = SimpleQueue()
        self.after_merge_queue:SimpleQueue[Tuple[str,List[DataContext]]] = SimpleQueue()
        self.merge_win_size = self.operator_params.get('merge_win_size',10)
        self.merge_cache_data_collection:Dict[str,Dict[int,List[DataContext]]] = {}
        self._init_plugin()

    def _init_plugin(self):
        """初始化插件"""
        try:
            module = importlib.import_module(self.module_name)
            for data_tag, parameter in self.parameters.items():
                execute_instance:PluginBase = module.plugin(self.global_config)
                execute_instance.initial_work(parameter)
                self.plugin_collections[data_tag] = execute_instance
        except Exception as e:
            error(traceback.format_exc())
            error("算子节点id",self.node_id, "插件初始化失败 -->", e, "插件模块名称", self.module_name)

    def _subscribe_all_data_from_source_queue(self):
        for queue in self.source_queue:
            t_pool_manager.add_task(task_name="transform_task",
                                    task_function=self._subscribe_data,
                                    args=(queue,),
                                    is_interval=True)

    def _subscribe_data(self,queue: SimpleQueue[List[DataContext]]) -> None:
        """
        订阅数据并且发送到合并组合队列
        :param queue:
        :return:
        """
        data:List[DataContext] = queue.get()
        for data_ctx in data:
            self.before_merge_queue.put(data_ctx)

    def merge_task(self):
        """
        合并任务，通过
        Dict[data_tag, Dict[watermark, List[DataContext]]]
        进行缓存，并且不停推进水位线
        :return:
        """
        data_ctx:DataContext = self.before_merge_queue.get()
        data_tag = data_ctx.get_data_tag()
        data_watermark = data_ctx.get_watermark()
        if data_tag not in self.merge_cache_data_collection:
            self.merge_cache_data_collection[data_tag] = {}
        if data_watermark not in self.merge_cache_data_collection[data_tag]:
            self.merge_cache_data_collection[data_tag][data_watermark] = []
        if len(self.merge_cache_data_collection[data_tag]) >= self.merge_win_size:
            self.merge_cache_data_collection[data_tag].pop(min(self.merge_cache_data_collection[data_tag].keys()))
            # 清理时间戳最小的那条数据
        self.merge_cache_data_collection[data_tag][data_watermark].append(data_ctx)
        self.__merge_data()

    def __merge_data(self):
        """
        合并数据
        :return:
        """
        can_pop_keys:List[Tuple[str,int]] = []
        for data_tag in self.merge_cache_data_collection:
            for data_watermark in self.merge_cache_data_collection[data_tag]:
                data_ctx_per_watermark = self.merge_cache_data_collection[data_tag][data_watermark]
                if len(data_ctx_per_watermark) == len(self.source_queue):
                    can_pop_keys.append((data_tag, data_watermark))
        for data_tag, data_watermark in can_pop_keys:
            merged_data_ctx_list = self.merge_cache_data_collection[data_tag].pop(data_watermark)
            self.after_merge_queue.put((data_tag,merged_data_ctx_list))

    def execute_plugin(self):
        data_tag, data_ctx_list =  self.after_merge_queue.get()
        self.__execute(data_tag, data_ctx_list)

    def __execute(self,data_tag:str, data_context_list:List[DataContext]):
        """
        执行插件内脚本内容
        :param data_tag:
        :param data_context_list:
        :return:
        """
        try:
            plugin = self.plugin_collections.get(data_tag, None)
            if plugin is None:
                warn(data_tag,"未找到对应插件, 合并后无法进一步处理, 直接传入下游")
                self.target_queue[0].put(data_context_list)
                return
            plugin.run(data_context_list)
            result = plugin.get_results()
            self.target_queue[0].put(result)
        except Exception as e:
            error(traceback.format_exc())
            error("合并算子内插件执行错误--->", e,"算子节点id",self.node_id)


        except Exception as e:
            error(traceback.format_exc())














