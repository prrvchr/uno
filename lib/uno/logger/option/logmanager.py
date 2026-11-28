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

from .logmodel import LogModel

from .logview import LogWindow

from .loghandler import WindowHandler

from ...logger.listener import LoggerListener

from ...runner.listener import DispatchListener

from ...logger.helper import showLogger

from contextlib import contextmanager


class LogManager():
    def __init__(self, ctx, window):
        self._ctx = ctx
        self._model = LogModel(ctx, LoggerListener(self.updateLoggers))
        self._view = LogWindow(ctx, window, WindowHandler(self))
        self._update = True

    @contextmanager
    def _noUpdate(self):
        self._update = False
        try:
            yield
        finally:
            self._update = True

# LogManager getter methods
    def getLayout(self):
        return self._view.getLayout()

# LogManager setter methods
    def dispose(self):
        self._model.dispose()
        self._view.dispose()

    # LogManager setter methods called by OptionManager
    def init(self):
        with self._noUpdate():
            self._view.initView(self._model.initLoggers())
            self._view.setLogSetting(*self._model.getLoggerSetting(self._view.getLogger()))

    def saveSetting(self):
        return self._model.saveSetting()

    # LogManager setter methods called by LoggerListener
    def updateLoggers(self):
        name = self._view.getLogger()
        loggers = self._model.getLoggerNames()
        self._view.updateLoggers(loggers)
        if name in loggers:
            with self._noUpdate():
                self._view.setLogger(name)

    # LogManager setter methods called by WindowHandler
    def setLogger(self, name):
        with self._noUpdate():
            self._view.setLogSetting(*self._model.getLoggerSetting(name))

    def enableLogger(self, enabled):
        if self._update:
            self._model.enableLogger(self._view.getLogger(), self._view.getLogLevel(), enabled)
        self._view.enableLogger(enabled)

    def toggleHandler(self, index):
        if self._update:
            self._model.toggleHandler(self._view.getLogger(), index)
        self._view.enableViewer(index == 2)

    def setLevel(self, level):
        if self._update:
            self._model.setLevel(self._view.getLogger(), level)

    def viewLog(self):
        name = self._view.getLogger()
        logger = self._model.getLogger(name)
        showLogger(self._ctx, Dispatch=DispatchListener(self), Name=name, Logger=logger)

    def dispatchFinished(self, _notification):
        print("LogManager.dispatchFinished() 1")
