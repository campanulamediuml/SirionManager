import traceback
from queue import SimpleQueue
from typing import List, Dict, Any, Optional

from SirionDep.sirion_dep_frame.data_object_frame.data_object import DataContext
from sirion_manager_logger.logger import info, error
from sirion_manager_operators.merge import MergeOperator
from sirion_manager_operators.operator_base.base import OperatorBase
from sirion_manager_operators.sink import SinkOperator
from sirion_manager_operators.source import SourceOperator
from sirion_manager_operators.split import SplitOperator
from sirion_manager_operators.transform import TransformOperator
from sirion_manager_type_template.dag_defination import DAGConfig, TypeDAGEdge, TypeDAGNode, PluginParams, \
    TypeOperatorType

TypeOperator = TransformOperator | SourceOperator | SinkOperator | MergeOperator | SplitOperator

operator_index: Dict[TypeOperatorType, type[TypeOperator]] = {
    "source": SourceOperator,
    "sink": SinkOperator,
    "merge": MergeOperator,
    "split": SplitOperator,
    "transform": TransformOperator,
}


class DAGNodeBuilder():
    def __init__(self, node_config: TypeDAGNode, global_config: Dict[str, Any]):
        self.node_id: str = node_config['node_id']
        self.node_type: str = node_config['node_type']
        self.module_name: str = node_config['module_name']
        self.operator_type: TypeOperatorType = node_config['operator_type']
        self.operator_params: Dict[str, Any] = node_config['operator_params']
        self.plugin_params: PluginParams = node_config['plugin_params']
        self.global_config: Dict[str, Any] = global_config
        self.target_edges: List[SimpleQueue[List[DataContext]]] = []
        self.source_edges: List[SimpleQueue[List[DataContext]]] = []
        self.operator: Optional[OperatorBase] = None

    def add_target_edge(self, edge_queue: SimpleQueue[List[DataContext]]) -> None:
        self.target_edges.append(edge_queue)

    def add_source_edge(self, edge_queue: SimpleQueue[List[DataContext]]):
        self.source_edges.append(edge_queue)

    def operator_init(self):
        """
        初始化算子
        :return:
        """
        self.operator = operator_index[self.operator_type](
            self.node_id,
            self.node_type,
            self.module_name,
            self.plugin_params,
            self.operator_params,
            self.global_config,
            self.source_edges,
            self.target_edges,
        )
        info("算子初始化", self.node_id, self.node_type, self.module_name, self.operator_type)


class DAGEdgeBuilder():
    def __init__(self, node_config: TypeDAGEdge, global_config: Dict[str, Any]):
        self.edge_id: str = node_config['edge_id']
        self.source_id: str = node_config['source_node']
        self.target_id: str = node_config['target_node']
        self.source_node: Optional[DAGNodeBuilder] = None
        self.target_node: Optional[DAGNodeBuilder] = None
        self.global_config: Dict[str, Any] = global_config
        self.edge_queue: SimpleQueue[List[DataContext]] = SimpleQueue()

    def add_source_node(self, node: DAGNodeBuilder):
        self.source_node = node

    def add_target_node(self, node: DAGNodeBuilder):
        self.target_node = node


class DAGBuilder():
    def __init__(self, dag_json_obj: DAGConfig):
        self.dag_json_obj: DAGConfig = dag_json_obj
        self._node_config_list = self.dag_json_obj['nodes']
        self._edge_config_list = self.dag_json_obj['edges']
        self.global_config: Dict[str, Any] = dag_json_obj['global_config']
        self._node_collections: Dict[str, DAGNodeBuilder] = {}
        self._edge_collections: Dict[str, DAGEdgeBuilder] = {}
        self.build_node()
        self.build_edge()
        self.update_all_nodes()
        self.update_all_edges()
        self.init_operator_by_node()

    def init_operator_by_node(self):
            for node_obj in self._node_collections.values():
                try:
                    node_obj.operator_init()
                except Exception as e:
                    error("算子初始化失败--->",e,node_obj.node_id)
                    error(traceback.format_exc())
                raise Exception("算子初始化失败，退出程序")

    def build_node(self):
        """
        初始化所有节点结构
        :return:
        """
        for node in self._node_config_list:
            node_id = node['node_id']
            node_obj = DAGNodeBuilder(node, self.global_config)
            self._node_collections[node_id] = node_obj

    def build_edge(self):
        """
        初始化所有边结构
        :return:
        """
        for edge in self._edge_config_list:
            edge_id = edge['edge_id']
            edge_obj = DAGEdgeBuilder(edge, self.global_config)
            self._edge_collections[edge_id] = edge_obj

    def update_all_edges(self):
        """
        关联边和节点的关系
        :return:
        """
        for edge_obj in self._edge_collections.values():
            source_node = self._node_collections[edge_obj.source_id]
            target_node = self._node_collections[edge_obj.target_id]
            edge_obj.add_source_node(source_node)
            edge_obj.add_target_node(target_node)

    def update_all_nodes(self):
        """
        关联节点和节点的出入队列
        :return:
        """
        for edge_obj in self._edge_collections.values():
            source_node = self._node_collections[edge_obj.source_id]
            target_node = self._node_collections[edge_obj.target_id]
            source_node.add_target_edge(edge_obj.edge_queue)
            target_node.add_source_edge(edge_obj.edge_queue)
