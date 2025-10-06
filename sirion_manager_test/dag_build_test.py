# !/usr/bin/env python
# -*-coding:utf-8 -*-
# Author     ：Campanula 梦芸 何
from typing import List

from sirion_manager_type_template.dag_defination import DAGConfig, TypeDAGNode

node_template:TypeDAGNode = {
    "node_id":"1",
    "node_type":"node",
    "module_name":"test_module",
    "operator_type":"operator",
    "operator_params":{},
    "plugin_params":{
        "common_param":{},
        "parameter":{}
    }
}

dag_obj_test: DAGConfig = {
    "global_config": {},
    "nodes":[

    ],
    "edges":[

    ]
}
