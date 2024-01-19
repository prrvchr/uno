#!
# -*- coding: utf-8 -*-

"""
╔════════════════════════════════════════════════════════════════════════════════════╗
║                                                                                    ║
║   Copyright (c) 2020 https://prrvchr.github.io                                     ║
║                                                                                    ║
║   Permission is hereby granted, free of charge, to any person obtaining            ║
║   a copy of this software and associated documentation files (the "Software"),     ║
║   to deal in the Software without restriction, including without limitation        ║
║   the rights to use, copy, modify, merge, publish, distribute, sublicense,         ║
║   and/or sell copies of the Software, and to permit persons to whom the Software   ║
║   is furnished to do so, subject to the following conditions:                      ║
║                                                                                    ║
║   The above copyright notice and this permission notice shall be included in       ║
║   all copies or substantial portions of the Software.                              ║
║                                                                                    ║
║   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,                  ║
║   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES                  ║
║   OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.        ║
║   IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY             ║
║   CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,             ║
║   TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE       ║
║   OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.                                    ║
║                                                                                    ║
╚════════════════════════════════════════════════════════════════════════════════════╝
"""

import uno
import unohelper

from com.sun.star.logging.LogLevel import INFO
from com.sun.star.logging.LogLevel import SEVERE

from com.sun.star.uno import Exception as UnoException

from .unotool import getConfiguration

from .configuration import g_identifier
from .configuration import g_synclog

from .logger import getLogger
g_basename = 'Replicator'

from threading import Thread
import traceback


class Replicator(Thread):
    def __init__(self, ctx, database, provider, users, sync):
        Thread.__init__(self)
        self._ctx = ctx
        self._database = database
        self._provider = provider
        self._config = getConfiguration(ctx, g_identifier, False)
        self._users = users
        self._sync = sync
        self.start()

    # XRestReplicator
    def dispose(self):
        print("replicator.dispose() 1")
        self._canceled = True
        self._sync.set()
        self.join()
        print("replicator.dispose() 2")

    def run(self):
        logger = getLogger(self._ctx, g_synclog, g_basename)
        try:
            logger.logprb(INFO, 'Replicator', 'run()', 101)
            while not self._canceled:
                timeout = self._config.getByName('ReplicateTimeout')
                logger.logprb(INFO, 'Replicator', 'run()', 102, timeout // 60)
                self._sync.clear()
                self._sync.wait(timeout)
                if self._canceled:
                    continue
                users, pages, total = self._synchronize(logger)
                logger.logprb(INFO, 'Replicator', 'run()', 103, users, pages, total)
                if total > 0:
                    users, pages, total = self._finalize(logger)
                    logger.logprb(INFO, 'Replicator', 'run()', 104, users, pages, total)
                self._database.dispose()
            logger.logprb(INFO, 'Replicator', 'run()', 105)
        except UnoException as e:
            logger.logprb(SEVERE, 'Replicator', 'run()', 106, e.Message)
        except Exception as e:
            logger.logprb(SEVERE, 'Replicator', 'run()', 107, e, traceback.format_exc())

    def _synchronize(self, logger):
        users = pages = count = 0
        try:
            for user in self._users.values():
                if self._canceled:
                    break
                if not user.hasSession():
                    continue
                if user.isOffLine():
                    logger.logprb(INFO, 'Replicator', '_synchronize()', 111)
                else:
                    users += 1
                    logger.logprb(INFO, 'Replicator', '_synchronize()', 112, user.Name)
                    for book in user.getBooks():
                        if self._canceled:
                            break
                        if book.isNew():
                            pages, count, args = self._provider.firstPullCard(self._database, user, book, pages, count)
                        else:
                            pages, count, args = self._provider.pullCard(self._database, user, book, pages, count)
                        if args is not None:
                            logger.logprb(SEVERE, *args)
                    logger.logprb(INFO, 'Replicator', '_synchronize()', 113, user.Name)
        except UnoException as e:
            logger.logprb(SEVERE, 'Replicator', '_synchronize()', 114, e.Message)
        except Exception as e:
            logger.logprb(SEVERE, 'Replicator', '_synchronize()', 115, e, traceback.format_exc())
        return users, pages, count

    def _finalize(self, logger):
        users = pages = count = 0
        try:
            self._provider.parseCard(self._database)
            for user in self._users.values():
                if self._canceled:
                    break
                if not user.hasSession():
                    continue
                if user.isOffLine():
                    logger.logprb(INFO, 'Replicator', '_finalize()', 121)
                else:
                    users += 1
                    logger.logprb(INFO, 'Replicator', '_finalize()', 122, user.Name)
                    for book in user.getBooks():
                        if self._canceled:
                            break
                        pages, count, args = self._provider.syncGroups(self._database, user, book, pages, count)
                        if args is not None:
                            logger.logprb(SEVERE, *args)
                    logger.logprb(INFO, 'Replicator', '_finalize()', 123, user.Name)
            if not self._canceled:
                self._database.syncGroups(stop)
        except UnoException as e:
            logger.logprb(SEVERE, 'Replicator', '_finalize()', 124, e.Message)
        except Exception as e:
            logger.logprb(SEVERE, 'Replicator', '_finalize()', 125, e, traceback.format_exc())
        return users, pages, count

