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

from ...unotool.layout import Layout

from ...unotool import getContainerWindow

from ...configuration import g_identifier


class LogWindow():
    def __init__(self, ctx, window, handler):
        self._window = getContainerWindow(ctx, window.getPeer(), handler, g_identifier, 'LogOption')
        self._layout = Layout(self._window, **self._getLayouts())
        self._window.setVisible(True)

# LogWindow getter methods
    def getLayout(self):
        return self._layout

    def getLogger(self):
        return self._getLoggers().getSelectedItem()

    def getLogLevel(self):
        return self._getLevel().getSelectedItemPos()

# LogWindow setter methods
    def dispose(self):
        if self._layout:
            self._layout.dispose()
            self._layout = None
        if self._window:
            self._window.dispose()
            self._window = None

    def initView(self, loggers):
        control = self._getLoggers()
        position = control.getSelectedItemPos()
        control.Model.StringItemList = loggers
        index = position if position != -1 else 0
        control.selectItemPos(index, True)

    def setLogger(self, logger):
        self._getLoggers().selectItem(logger, True)

    def updateLoggers(self, loggers):
        self._getLoggers().Model.StringItemList = loggers

    def setLogSetting(self, enabled, level, index):
        self._getHandler(index).State = 1
        self._getLevel().selectItemPos(level, True)
        self.enableLogger(enabled)

    def enableViewer(self, enabled):
        self._getViewer().Model.Enabled = enabled

    def enableLogger(self, enabled):
        self._getLogger().State = int(enabled)
        self._getLevelLabel().Model.Enabled = enabled
        self._getLevel().Model.Enabled = enabled
        self._getConsoleHandler().Model.Enabled = enabled
        self._getOutputLabel().Model.Enabled = enabled
        control = self._getFileHandler()
        control.Model.Enabled = enabled
        self.enableViewer(enabled and control.State)

    def _getLayouts(self):
        return {'horizontals': self._getHorizontals(),
                'tops': self._getTops()}

    def _getHorizontals(self):
        yield self._getFixedLine()
        yield self._getLoggers()
        yield self._getLevel()

    def _getTops(self):
        yield self._getLogger()

# LogWindow private control methods
    def _getFixedLine(self):
        return self._window.getControl('FixedLine1')

    def _getLoggers(self):
        return self._window.getControl('ListBox1')

    def _getLogger(self):
        return self._window.getControl('CheckBox1')

    def _getOutputLabel(self):
        return self._window.getControl('Label2')

    def _getLevelLabel(self):
        return self._window.getControl('Label3')

    def _getLevel(self):
        return self._window.getControl('ListBox2')

    def _getHandler(self, index):
        return self._window.getControl('OptionButton%s' % index)

    def _getConsoleHandler(self):
        return self._window.getControl('OptionButton1')

    def _getFileHandler(self):
        return self._window.getControl('OptionButton2')

    def _getViewer(self):
        return self._window.getControl('CommandButton1')
