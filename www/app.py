#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-04-08 10:10:40
# @Author  : pydoker (ldk_kdl@163.com)
# @Link    : ${link}
# @Version : $Id$

import logging
logging.basicConfig(level=logging.INFO)

import asyncio
import os
import json
import time
from datetime import datetime
from aiohttp import web


def index(request):
    return web.Request(body=b'<h1>Awesome</h1>', headers={'content-type': 'text/html'})


@asyncio.coroutine
def init(loop):
    app = web.Application(loop=loop)
    app.router.add_route('GET', '/', index)
    srv = yield from loop.create_server(app.make_handler(), '127.0.0.1', 9000)
    logging.info('server started at http://127.0.0.1:9000....')
    return srv


loop = asyncio.get_event_loop()
loop.run_until_complete(init(loop))
loop.run_forever()
