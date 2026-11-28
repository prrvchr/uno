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

import uno

from ...unotool.layout import Layout

from ...unotool import getContainerWindow
from ...unotool import getDesktop
from ...unotool import getDialog
from ...unotool import getDialogPosSize
from ...unotool import getTopWindow
from ...unotool import getWindowPosSize
from ...unotool import setDialogPosition
from ...unotool import setWindowPosSize

from ...configuration import g_identifier


class LogWindow():
    def __init__(self, ctx, parent, handler, listener, name, possize, title, enabled, roller):
        self._ctx = ctx
        self._length = 0
        if parent:
            self._frame = parent
            self._dialog = getDialog(ctx, g_identifier, 'LogDialog', parent=parent)
            window = self._dialog.getPeer()
        else:
            self._frame = getTopWindow(ctx, name)
            self._dialog = None
            window = self._frame.getContainerWindow()
        self._window = getContainerWindow(ctx, window, handler, g_identifier, 'LogWindow')
        if parent:
            setDialogPosition(ctx, self._dialog, parent, possize)
            self._dialog.Title = title
        else:
            # XXX: setComponent is needed if we want a StatusIndicator at the bottom
            self._frame.setComponent(self._window, None)
            self._frame.addCloseListener(listener)
            setWindowPosSize(ctx, window, self._window, possize)
            self._window.addWindowListener(Layout(self._window, **self._getLayouts()))
            self._frame.setTitle(title)
        self._getLogInfo().Model.Enabled = enabled
        self._getClearLog().Model.Enabled = roller
        self._window.setVisible(True)
        window.setVisible(True)

# LogWindow getter methods
    def getLayout(self):
        return self._layout

    def isModal(self):
        return self._dialog is not None

    def getWindowPosSize(self):
        if self.isModal():
            return getDialogPosSize(self._dialog, self._frame)
        return getWindowPosSize(self._frame.getContainerWindow())

# LogWindow setter methods
    def execute(self):
        if self.isModal():
            return self._dialog.execute()
        return 0

    def initLogger(self, text):
        control = self._getLogger()
        control.Text = text
        self._length = len(text)
        self._refreshLog(control)

    def updateLogger(self, msg):
        control = self._getLogger()
        selection = uno.createUnoStruct('com.sun.star.awt.Selection', self._length, self._length)
        control.insertText(selection, msg)
        self._length += len(msg)
        self._refreshLog(control)

    def setTitle(self, title):
        if self.isModal():
            self._dialog.Title = title
        else:
            self._frame.setTitle(title)

    def enableLogInfo(self, enabled):
        self._getLogInfo().Model.Enabled = enabled

    def enableButtons(self, enabled):
        self._getLogInfo().Model.Enabled = enabled
        self._getCloseLog().Model.Enabled = enabled

    def close(self, runnable):
        if self.isModal():
            self._dialog.endDialog(runnable)
        else:
            self._frame.close(True)

    def dispose(self):
        if not self.isModal():
            self._frame.setComponent(None, None)
            self._frame.getContainerWindow().dispose()
            getDesktop(self._ctx).getFrames().remove(self._frame)
            self._frame = None

# LogWindow private methods
    def _getLayouts(self):
        return {'controls': self._getControls(),
                'bottoms': self._getBottoms(),
                'lefts': self._getLefts()}

    def _getControls(self):
        yield self._getLogger()

    def _getBottoms(self):
        yield self._getCloseLog()

    def _getLefts(self):
        yield self._getLogInfo()

    def _refreshLog(self, control):
        selection = uno.createUnoStruct('com.sun.star.awt.Selection', self._length, self._length)
        control.setSelection(selection)

# LogWindow private control methods
    def _getLogger(self):
        return self._window.getControl('TextField1')

    def _getLogInfo(self):
        return self._window.getControl('CommandButton1')

    def _getClearLog(self):
        return self._window.getControl('CommandButton2')

    def _getCloseLog(self):
        return self._window.getControl('CommandButton3')
