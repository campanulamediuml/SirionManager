# !/usr/bin/env python
# -*-coding:utf-8 -*-
# Author     ：Campanula 梦芸 何
from SirionDep.sirion_dep_util_tools.logger.logger import dep_dbg, dep_info, dep_warn, dep_error

LOG_SOURCE = "MNG"


def dbg(*msg):
    dep_dbg(LOG_SOURCE, *msg)


def info(*msg):
    dep_info(LOG_SOURCE, *msg)


def warn(*msg):
    dep_warn(LOG_SOURCE, *msg)


def error(*msg):
    dep_error(LOG_SOURCE, *msg)
