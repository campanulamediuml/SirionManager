# !/usr/bin/env python
# -*-coding:utf-8 -*-
import os
import time
import traceback

from SirionDep.sirion_dep_init_env.init import env_config
from sirion_manager_logger.logger import dbg, error


if __name__ == '__main__':
    print(env_config)
    try:
        res = 3/0
        print(res)
    except Exception as e:
        err = traceback.format_exc()
        error(err)
    dbg("发送错误信息")
    time.sleep(10)
