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

from com.sun.star.logging.LogLevel import INFO

from .logmodel import LogModel

from .logview import LogWindow

from .loghandler import CloseListener

from .loghandler import WindowHandler

from ..listener import LoggerListener

from ..loghandler import ViewHandler

from ...unotool import notifyDispatch


class LogManager():
    def __init__(self, ctx, source, logger, parent=None, name=None, dispatch=None):
        self._ctx = ctx
        self._source = source
        self._disposed = False
        self._dispatch = dispatch
        self._model = LogModel(ctx, logger, name)
        self._listener = CloseListener(self)
        self._view = LogWindow(ctx, parent, WindowHandler(self), self._listener, name, *self._model.getViewData())
        self._mlistener = LoggerListener(self.refresh)
        logger.addModifyListener(self._mlistener)
        self._view.initLogger(self._model.getLoggerContent())
        self._handler = ViewHandler(ctx, self.publish)
        logger.addLogHandler(self._handler)

    def publish(self, text):
        if self._disposed:
            return
        self._view.updateLogger(text)

    def refresh(self):
        if self._disposed:
            return
        url = self._model.refreshUrl()
        self._view.setUrl(url)
        self._view.initLogger(self._model.getLoggerContent())

# XCloseListener
    def queryClosing(self, _source, _ownership):
        self._disposed = True
        self._dispose()

    def notifyClosing(self, source):
        source.removeCloseListener(self._listener)
        self._view.dispose()

    def logInfos(self):
        self._model.logInfos(INFO, 'LogManager', 'logInfos')

    def clearLog(self):
        self._model.clearLogger()
        self._view.initLogger('')

    def closeLog(self):
        self._disposed = True
        self._close()

    def _close(self):
        if self._view.isModal():
            self._dispose()
        self._view.close(True)

    def _dispose(self):
        self._view.enableButtons(False)
        self._logger.removeLogHandler(self._handler)
        self._logger.removeModifyListener(self._mlistener)
        self._model.savePosSize(self._view.getWindowPosSize())
        if self._dispatch:
            notifyDispatch(self._source, self._dispatch)
