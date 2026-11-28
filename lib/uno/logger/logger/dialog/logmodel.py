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

from com.sun.star.awt import Rectangle

from com.sun.star.logging.LogLevel import SEVERE

from ..logwrapper import LogWrapper

from ..helper import getLoggerUrl

from ...setup.task.helper import isLinuxDistribution
from ...setup.task.helper import parseRequirements

from ...unotool import getConfiguration
from ...unotool import getStreamSequence
from ...unotool import getPathSubstitution
from ...unotool import getResourceLocation
from ...unotool import getSimpleFile

from ...config.configuration import g_requirements

from ...configuration import g_identifier

from importlib import metadata
from packaging.requirements import Requirement
import os
import sys
import sysconfig
import traceback


class LogModel():
    def __init__(self, ctx, logger, name):
        self._ctx = ctx
        self._name = name
        self._sf = getSimpleFile(ctx)
        self._logger = LogWrapper(ctx, basename='Logger', logger=logger)
        self._config = getConfiguration(ctx, g_identifier, True)
        self._url, self._roller = getLoggerUrl(ctx, logger.Name)

# Public getter method
    def getViewData(self):
        return self._getLogPosSize(), self._getTitle(), self._isLoggerLoaded(), self._roller

    def getLoggerContent(self, url=None):
        target = url or self._url
        if target and self._sf.exists(target):
            return getStreamSequence(self._sf.openFileRead(target)).value.decode('utf-8')
        return ''

# Public setter method
    def clearLogger(self):
        if self._roller and self._url and self._sf.exists(self._url):
            self._sf.kill(self._url)

    def savePosSize(self, possize):
        loggers = self._config.getByName('LogPosSizes')
        value = uno.Any('[]long', (possize.X, possize.Y, possize.Width, possize.Height))
        if loggers.hasByName(self._name):
            node = loggers.getByName(self._name)
        else:
            node = loggers.createInstance()
            loggers.insertByName(self._name, node)
        uno.invoke(node, 'replaceByName', ('LogPosSize', value))
        self._config.commitChanges()

    def logInfos(self, level, cls, mtd):
        self._logger.logprb(level, cls, mtd, 121, sys.version)
        if isLinuxDistribution(self._ctx):
            self._logger.logprb(level, cls, mtd, 122, sys.executable)
        else:
            url = getPathSubstitution(self._ctx, '$(prog)')
            path = os.path.realpath(uno.fileUrlToSystemPath(url))
            self._logger.logprb(level, cls, mtd, 123, path)
        self._logger.logprb(level, cls, mtd, 124, sysconfig.get_config_var('EXT_SUFFIX'))
        self._logger.logprb(level, cls, mtd, 125, os.pathsep.join(sys.path))
        # If a requirements file exists at the extension root,
        # then we check if the requirements are met
        url = getResourceLocation(self._ctx, g_identifier, g_requirements)
        if getSimpleFile(self._ctx).exists(url):
            self._logRequirements(level, cls, mtd, url)

# Private setter method
    def _logRequirements(self, default, cls, mtd, url):
        info = sys.version_info
        ver = '%s.%s.%s' % (info.major, info.minor, info.micro)
        for requirement in parseRequirements(url):
            name = requirement.name
            level = default
            try:
                data = self._getMetadata(name)
                if data is None:
                    level = SEVERE
                    msg = self._getMissingMessage(name)
                else:
                    msg = self._getPackageMessage(requirement, name, data, ver)
            except Exception as e:
                msg = self._logger.resolveString(137, name, e, traceback.format_exc())
                level = SEVERE
            self._logger.logp(level, cls, mtd, msg)

# Private getter method
    def _getLogPosSize(self):
        loggers = self._config.getByName('LogPosSizes')
        if loggers.hasByName(self._name):
            return Rectangle(*loggers.getByName(self._name).getPropertyValue('LogPosSize'))
        return Rectangle(-1, -1, -1, -1)

    def _getTitle(self):
        url = self._url if self._url else "no Logger"
        return f'{self._name} - {url}'

    def _isLoggerLoaded(self):
        return self._url is not None

    def _getMetadata(self, name):
        try:
            return metadata.metadata(name)
        except metadata.PackageNotFoundError:
            return None

    def _getPackageMessage(self, requirement, name, data, ver):
        dver = data.get('Version')
        pver = data.get('Requires-Python')
        if pver is None:
            pver = '>=' + ver

        location = None
        distfiles = metadata.files(name)
        if distfiles:
            location = str(distfiles[0].locate().parent.parent)

        if location:
            req = Requirement('python' + pver)
            is_version_match = requirement.specifier.contains(dver)
            is_python_match = req.specifier.contains(ver)

            rver = str(requirement.specifier) if requirement.specifier else ""
            if is_version_match:
                if is_python_match:
                    msg = self._logger.resolveString(131, name, dver, location)
                else:
                    msg = self._logger.resolveString(132, name, dver, pver, ver, location)
            elif is_python_match:
                msg = self._logger.resolveString(133, name, dver, rver, location)
            else:
                msg = self._logger.resolveString(134, name, dver, pver, rver, ver, location)
        else:
            rver = str(requirement.specifier) if requirement.specifier else ""
            msg = self._logger.resolveString(135, name, dver, rver)
        return msg

    def _getMissingMessage(self, name):
        return self._logger.resolveString(136, name)
