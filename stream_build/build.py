from typing import List, Dict
import json
import os


class DAGBuild:
    """
    从 JSON 文件读取 DAG 定义，并提供拓扑排序功能。
    排序结果保存在 self.dag_sorted_node: List[str] 中。
    """

    def __init__(self, json_path: str):
        """
        参数
        ----
        json_path: str
            符合 TypedDict 定义的 DAG JSON 文件路径
        """
        if not os.path.isfile(json_path):
            raise FileNotFoundError(f"JSON file not found: {json_path}")

        with open(json_path, encoding="utf-8") as f:
            self._raw: Dict = json.load(f)

        # 基本校验
        if "nodes" not in self._raw or "edges" not in self._raw:
            raise ValueError("JSON must contain 'nodes' and 'edges'")

        self.nodes: List[Dict] = self._raw["nodes"]
        self.edges: List[Dict] = self._raw["edges"]

        # 排序结果
        self.dag_sorted_node: List[str] = []

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------
    def dag_sort(self) -> List[str]:
        """
        使用 Kahn 算法对 DAG 进行拓扑排序。
        返回节点 ID 的有序列表，并写入 self.dag_sorted_node
        """
        # 1. 建图
        adj: Dict[str, List[str]] = {n["node_id"]: [] for n in self.nodes}
        indeg: Dict[str, int] = {n["node_id"]: 0 for n in self.nodes}

        for e in self.edges:
            u, v = e["source_node"], e["target_node"]
            adj[u].append(v)
            indeg[v] += 1

        # 2. 零入度队列
        q = [nid for nid, d in indeg.items() if d == 0]
        order: List[str] = []

        # 3. BFS
        while q:
            u = q.pop(0)
            order.append(u)
            for v in adj[u]:
                indeg[v] -= 1
                if indeg[v] == 0:
                    q.append(v)

        if len(order) != len(self.nodes):
            raise RuntimeError("DAG 中存在环，无法完成拓扑排序")

        self.dag_sorted_node = order
        return order


# --------------------- 简单测试 ---------------------
if __name__ == "__main__":
    # 假设 dag.json 就是之前给出的完整 JSON
    dag = DAGBuild("dag.json")
    print("拓扑序:", dag.dag_sort())
    print("内部缓存:", dag.dag_sorted_node)