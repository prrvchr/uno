#!
# -*- coding: utf-8 -*-

"""
╔════════════════════════════════════════════════════════════════════════════════════╗
║                                                                                    ║
║   Copyright (c) 2020-25 https://prrvchr.github.io                                  ║
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

from com.sun.star.logging.LogLevel import SEVERE
from com.sun.star.logging.LogLevel import WARNING
from com.sun.star.logging.LogLevel import INFO
from com.sun.star.logging.LogLevel import CONFIG
from com.sun.star.logging.LogLevel import FINE
from com.sun.star.logging.LogLevel import FINER
from com.sun.star.logging.LogLevel import FINEST
from com.sun.star.logging.LogLevel import ALL
from com.sun.star.logging.LogLevel import OFF

from ...logger.loggerpool import LoggerPool

from ...logger.helper import getAvailableLoggers

from ...unotool import getConfiguration

from ...configuration import g_identifier


class LogModel():
    def __init__(self, ctx, listener):
        self._ctx = ctx
        self._listener = listener
        self._pool = LoggerPool(ctx)
        self._pool.addModifyListener(listener)
        self._config = getConfiguration(ctx, '/org.openoffice.Office.Logging/Settings', True)
        self._settings = {}

# Public getter method
    def initLoggers(self):
        self._settings = {}
        return self.getLoggerNames()

    def getLoggerNames(self):
        return getAvailableLoggers(self._config, g_identifier)

    def getLogger(self, name):
        return self._pool.getNamedLogger(name)

    def getLoggerSetting(self, name):
        return self._getLoggerSetting(name)

    def saveSetting(self):
        for name in self._settings:
            level, handler = self._settings[name]
            if not self._config.hasByName(name):
                continue
            config = self._config.getByName(name)
            if level != config.LogLevel:
                config.LogLevel = level
            if handler != config.DefaultHandler:
                config.DefaultHandler = handler
        if self._config.hasPendingChanges():
            self._config.commitChanges()
            return True
        return False

# Public setter method
    def dispose(self):
        self._pool.removeModifyListener(self._listener)

    def enableLogger(self, name, level, enabled):
        self._settings[name][0] = self._getLogLevels(level) if enabled else OFF

    def setLevel(self, name, level):
        self._settings[name][0] = self._getLogLevels(level)

    def toggleHandler(self, name, index):
        self._settings[name][1] = self._getLogHandler(index)

# Private getter method
    def _getLoggerSetting(self, name):
        if name in self._settings:
            level, handler = self._settings[name]
        elif self._config.hasByName(name):
            config = self._config.getByName(name)
            level = config.LogLevel
            handler = config.DefaultHandler
            self._settings[name] = [level, handler]
        else:
            return False, self._getDefaultIndex(), self._getDefaultHandler()
        return level != OFF, self._getLevelIndex(level), self._getHandlerIndex(handler)

    def _getLogLevels(self, level):
        return self._logLevels()[level]

    def _getLevelIndex(self, level):
        if level in self._logLevels():
            index = self._logLevels().index(level)
        else:
            index = self._getDefaultIndex()
        return index

    def _getDefaultIndex(self):
        return len(self._logLevels()) - 1

    def _getDefaultHandler(self):
        return 1

    def _getLogHandler(self, index):
        return self._logHandlers()[index - 1]

    def _getHandlerIndex(self, handler):
        return self._logHandlers().index(handler) + 1

    def _logHandlers(self):
        return ('com.sun.star.logging.ConsoleHandler',
                'com.sun.star.logging.FileHandler')

    def _logLevels(self):
        return (SEVERE,
                WARNING,
                INFO,
                CONFIG,
                FINE,
                FINER,
                FINEST,
                ALL)
