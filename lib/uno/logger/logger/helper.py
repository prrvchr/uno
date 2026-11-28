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

from .logwrapper import LogWrapper

from ..unotool import executeDesktopDispatch
from ..unotool import getConfiguration
from ..unotool import getPathSubstitution
from ..unotool import getResourceLocation

from ..config.configuration import g_folderlog

from ..configuration import g_basename
from ..configuration import g_defaultlog
from ..configuration import g_identifier


def getLogger(ctx, name=g_defaultlog, basename=g_basename, logger=None):
    return LogWrapper(ctx, name, basename, logger)


def getAvailableLoggers(settings, identifier):
    return sorted(name for name in settings.getElementNames() if name.startswith(identifier))


def showLogger(ctx, listener=None, /, **kwargs):  # noqa
    url = f'vnd.sun.star.job:service={g_identifier}.Logger'
    executeDesktopDispatch(ctx, url, listener, **kwargs)


def getLoggerUrl(ctx, name):
    setting = getConfiguration(ctx, '/org.openoffice.Office.Logging/Settings')
    if setting.hasByName(name):
        logger = setting.getByName(name)
        if logger.DefaultHandler == 'com.sun.star.logging.FileHandler':
            return getLoggerHandlerUrl(ctx, logger, name), False
    return getRollerHandlerUrl(ctx, name), True


def getLoggerHandlerUrl(ctx, logger, name):
    url = '$(userurl)/$(loggername).log'
    handler = logger.getByName('HandlerSettings')
    if handler.hasByName('FileURL'):
        url = handler.getByName('FileURL')
    url = url.replace('$(loggername)', name)
    return getPathSubstitution(ctx, url)


def getRollerHandlerUrl(ctx, name):
    path = '%s/%s.log' % (g_folderlog, name)
    return getResourceLocation(ctx, g_identifier, path)
