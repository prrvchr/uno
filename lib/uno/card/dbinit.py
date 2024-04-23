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

from com.sun.star.sdbc import INTEGER
from com.sun.star.sdbc import SMALLINT
from com.sun.star.sdbc import VARCHAR

from .unotool import checkVersion

from .dbtool import createStaticTables
from .dbtool import createTables
from .dbtool import createIndexes
from .dbtool import createForeignKeys
from .dbtool import getConnectionInfos
from .dbtool import getDataBaseTables
from .dbtool import getDataBaseIndexes
from .dbtool import getDataBaseForeignKeys
from .dbtool import getDataSourceCall
from .dbtool import getDataSourceConnection
from .dbtool import getDriverInfos
from .dbtool import getTableNames
from .dbtool import getTables
from .dbtool import getIndexes
from .dbtool import getForeignKeys

from .dbtool import getSequenceFromResult
from .dbtool import getDataFromResult

from .dbqueries import getSqlQuery

from .dbconfig import g_version
from .dbconfig import g_superuser
from .dbconfig import g_view
from .dbconfig import g_cardview
from .dbconfig import g_bookview
from .dbconfig import g_dba
from .dbconfig import g_drvinfos

import traceback


def getDataBaseConnection(ctx, url, user, pwd, new, infos=None):
    if new:
        infos = getDriverInfos(ctx, url, g_drvinfos)
    return getDataSourceConnection(ctx, url, user, pwd, new, infos)

def createDataBase(connection, odb, addressbook):
    tables = connection.getTables()
    statement = connection.createStatement()
    createStaticTable(tables, statement, g_csv, True)
    _createTables(connection, statement, tables)
    _createIndexes(statement, tables)
    _createForeignKeys(statement, tables)
    #_createRoleAndPrivileges(statement, tables, connection.getGroups())

    #tables = getTables(self._ctx, connection, self._version)
    #executeSqlQueries(statement, tables)
    #executeQueries(self._ctx, statement, getQueries())
    executeQueries(ctx, statement, _getQueries())
    views = _getViews(ctx, connection, addressbook)
    executeSqlQueries(statement, views)
    statement.close()
    connection.getParent().DatabaseDocument.storeAsURL(odb, ())

def _createTables(connection, statement, tables):
    infos = getConnectionInfos(connection, 'AutoIncrementCreation', 'RowVersionCreation')
    createTables(tables, getDataBaseTables(connection, statement, getTables(), getTableNames(), infos[0], infos[1]))

def _createIndexes(statement, tables):
    createIndexes(tables, getDataBaseIndexes(statement, getIndexes()))

def _createForeignKeys(statement, tables):
    createForeignKeys(tables, getDataBaseForeignKeys(statement, getForeignKeys()))

def _getAddressbookColumns(ctx, connection):
    columns = OrderedDict()
    call = getDataSourceCall(ctx, connection, 'getColumns')
    result = call.executeQuery()
    while result.next():
        index = result.getInt(1)
        name = result.getString(2)
        view = result.getString(3)
        print("DataBase._getAddressbookColumns() Index: %s - Name: %s - View: %s" % (index, name, view))
        if view is not None:
            if view not in columns:
                columns[view] = OrderedDict()
            columns[view][name] = index
    result.close()
    call.close()
    return columns


def getTables(ctx, connection, version=g_version):
    tables = []
    call = getDataSourceCall(ctx, connection, 'getTables')
    for table in _getTableNames(ctx, connection):
        view = False
        versioned = False
        columns = []
        primary = []
        unique = []
        constraint = []
        call.setString(1, table)
        result = call.executeQuery()
        while result.next():
            data = getDataFromResult(result)
            view = data.get('View')
            versioned = data.get('Versioned')
            column = data.get('Column')
            definition = '"%s" %s' % (column, data.get('Type'))
            default = data.get('Default')
            if default:
                definition += ' DEFAULT %s' % default
            options = data.get('Options')
            if options:
                definition += ' %s' % options
            columns.append(definition)
            if data.get('Primary'):
                primary.append('"%s"' % column)
            if data.get('Unique'):
                unique.append({'Table': table, 'Column': column})
            if data.get('ForeignTable') and data.get('ForeignColumn'):
                constraint.append({'Table': table,
                                   'Column': column,
                                   'ForeignTable': data.get('ForeignTable'),
                                   'ForeignColumn': data.get('ForeignColumn')})
        if primary:
            columns.append(getSqlQuery(ctx, 'getPrimayKey', primary))
        for format in unique:
            columns.append(getSqlQuery(ctx, 'getUniqueConstraint', format))
        for format in constraint:
            columns.append(getSqlQuery(ctx, 'getForeignConstraint', format))
        if checkVersion(version, g_version) and versioned:
            columns.append(getSqlQuery(ctx, 'getPeriodColumns'))
        format = (table, ','.join(columns))
        query = getSqlQuery(ctx, 'createTable', format)
        if checkVersion(version, g_version) and versioned:
            query += getSqlQuery(ctx, 'getSystemVersioning')
        tables.append(query)
        result.close()
    call.close()
    return tables

def _getTableNames(ctx, connection):
    statement = connection.createStatement()
    query = getSqlQuery(ctx, 'getTableNames')
    result = statement.executeQuery(query)
    tables = getSequenceFromResult(result)
    result.close()
    statement.close()
    return tables

def _getViews(ctx, connection, name):
    result = _getAddressbookColumns(ctx, connection)
    sel1 = []
    tab1 = []
    queries = []
    format = g_view

    q = 'CREATE VIEW IF NOT EXISTS "%(ViewName)s" AS SELECT %(ViewSelect)s FROM "%(CardTable)s" %(ViewTable)s'

    t1 = 'LEFT JOIN "%(ViewName)s" ON "%(CardTable)s"."%(CardColumn)s"="%(ViewName)s"."%(CardColumn)s"'
    t2 = 'LEFT JOIN "%(DataTable)s" AS "%(AliasNum)s" ON "%(CardTable)s"."%(CardColumn)s"="%(AliasNum)s"."%(CardColumn)s" '
    t2 += 'AND "%(AliasNum)s"."%(DataColumn)s"=%(ColumnId)s'

    s1 = '"%(ViewName)s"."%(ColumnName)s"'
    s2 = '"%(AliasNum)s"."%(DataValue)s" AS "%(ColumnName)s"'
    s3 = '"%(CardTable)s"."%(CardColumn)s"'
    s4 = '"%(CardTable)s"."Created","%(CardTable)s"."Modified"'
    s5 = '"%(CardTable)s"."%(CardUri)s"'

    for view, columns in result.items():
        i = 0
        col2 = columns.keys()
        sel2 = []
        tab2 = []
        format['ViewName'] = view
        for column, index in columns.items():
            format['ColumnName'] = column
            format['ColumnId'] = index
            format['AliasNum'] = i
            sel1.append(s1 % format)
            tab2.append(t2 % format)
            sel2.append(s2 % format)
            i += 1
        sel2.append(s3 % format)
        format['ViewSelect'] = ','.join(sel2)
        format['ViewTable'] = ' '.join(tab2)
        tab1.append(t1 % format)
        queries.append(q % format)
    sel1.append(s3 % format)
    sel1.append(s4 % format)
    sel1.append(s5 % format)
    format['ViewName'] = g_cardview
    format['ViewSelect'] = ','.join(sel1)
    format['ViewTable'] = ' '.join(tab1)
    queries.append(q % format)
    return queries



def getStaticTables():
    tables = ('Tables',
              'Columns',
              'TableColumn',
              'Resources',
              'Properties',
              'Types',
              'PropertyType')
    return tables

def _getQueries():
    return (('createSelectUser', None),
            ('createInsertUser', None),
            ('createInsertBook', None),
            ('createUpdateAddressbookName', None),
            ('createMergeCard', None),
            ('createMergeGroup', None),
            ('createMergeGroupMembers', None),
            ('createDeleteCard', None),
            ('createUpdateUser', None),
            ('createGetLastUserSync', None),
            ('createGetLastAddressbookSync', None),
            ('createGetLastGroupSync', None),
            ('createSelectChangedCards', None),
            ('insertSuperUser', g_superuser),
            ('insertSuperAdressbook', None),
            ('insertSuperGroup', None),
            ('createSelectColumns', None),
            ('createSelectColumnIds', None),
            ('createSelectPaths', None),
            ('createSelectLists', None),
            ('createSelectTypes', None),
            ('createSelectMaps', None),
            ('createSelectTmps', None),
            ('createSelectFields', None),
            ('createSelectGroups', None),
            ('createSelectCardGroup', None),
            ('createInitGroups', None),
            ('createInsertGroup', None),
            ('createMergeCardValue', None),
            ('createMergeCardData', None),
            ('createMergeCardGroup', None),
            ('createMergeCardGroups', None),
            ('createSelectChangedAddressbooks', None),
            ('createSelectChangedGroups', None),
            ('createUpdateAddressbook', None),
            ('createUpdateGroup', None),
            ('createSelectCardProperties', None))

def _getStaticTables():
    return {'Resources':   {'CatalogName': 'PUBLIC',
                            'SchemaName':  'PUBLIC',
                            'Type':        'TEXT TABLE',
                            'Columns': ({'Name': 'Resource',
                                         'TypeName': 'INTEGER',
                                         'Type': INTEGER,
                                         'IsNullable': NO_NULLS},
                                        {'Name': 'Path',
                                         'TypeName': 'VARCHAR',
                                         'Type': 12,
                                         'Scale': 100,
                                         'IsNullable': NO_NULLS},
                                        {'Name': 'Name',
                                         'TypeName': 'VARCHAR',
                                         'Type': 12,
                                         'Scale': 100,
                                         'IsNullable': NULLABLE,
                                         'DefaultValue': 'NULL'},
                                        {'Name': 'View',
                                         'TypeName': 'VARCHAR',
                                         'Type': 12,
                                         'Scale': 100,
                                         'IsNullable': NULLABLE,
                                         'DefaultValue': 'NULL'},
                                        {'Name': 'Method',
                                         'TypeName': 'SMALLINT',
                                         'Type': 12,
                                         'IsNullable': NULLABLE,
                                         'DefaultValue': 'NULL'}),
                            'PrimaryKeys': ('Resource', )},
            'Columns':     {'CatalogName': 'PUBLIC',
                            'SchemaName':  'PUBLIC',
                            'Type':        'TEXT TABLE',
                            'Columns': ({'Name': 'Column',
                                         'TypeName': 'INTEGER',
                                         'Type': INTEGER,
                                         'IsNullable': NO_NULLS},
                                        {'Name': 'Name',
                                         'TypeName': 'VARCHAR',
                                         'Type': 12,
                                         'Scale': 100,
                                         'IsNullable': NO_NULLS}),
                            'PrimaryKeys': ('Column', )},
            'TableColumn': {'CatalogName': 'PUBLIC',
                            'SchemaName':  'PUBLIC',
                            'Type':        'TEXT TABLE',
                            'Columns': ({'Name': 'Table',
                                         'TypeName': 'INTEGER',
                                         'Type': INTEGER,
                                         'IsNullable': NO_NULLS},
                                        {'Name': 'Column',
                                         'TypeName': 'INTEGER',
                                         'Type': INTEGER,
                                         'IsNullable': NO_NULLS},
                                        {'Name': 'TypeName',
                                         'TypeName': 'VARCHAR',
                                         'Type': 12,
                                         'Scale': 100,
                                         'IsNullable': NO_NULLS},
                                        {'Name': 'Type',
                                         'TypeName': 'INTEGER',
                                         'Type': INTEGER,
                                         'IsNullable': NO_NULLS},
                                        {'Name': 'Scale',
                                         'TypeName': 'INTEGER',
                                         'Type': INTEGER,
                                         'IsNullable': NULLABLE,
                                         'DefaultValue': 'NULL'},
                                        {'Name': 'IsNullable',
                                         'TypeName': 'INTEGER',
                                         'Type': INTEGER,
                                         'IsNullable': NO_NULLS,
                                         'DefaultValue': '0'},
                                        {'Name': 'DefaultValue',
                                         'TypeName': 'VARCHAR',
                                         'Type': 12,
                                         'Scale': 100,
                                         'IsNullable': NULLABLE,
                                         'DefaultValue': 'NULL'},
                                        {'Name': 'IsRowVersion',
                                         'TypeName': 'BOOLEAN',
                                         'Type': 16,
                                         'IsNullable': NULLABLE,
                                         'DefaultValue': 'NULL'},
                                        {'Name': 'IsAutoIncrement',
                                         'TypeName': 'BOOLEAN',
                                         'Type': 16,
                                         'IsNullable': NULLABLE,
                                         'DefaultValue': 'NULL'},
                                        {'Name': 'Primary',
                                         'TypeName': 'BOOLEAN',
                                         'Type': 16,
                                         'IsNullable': NULLABLE,
                                         'DefaultValue': 'NULL'}),
                            'PrimaryKeys': ('Table', 'Column')},
            'ForeignKeys': {'CatalogName': 'PUBLIC',
                            'SchemaName':  'PUBLIC',
                            'Type':        'TEXT TABLE',
                            'Columns': ({'Name': 'Table',
                                         'TypeName': 'INTEGER',
                                         'Type': 4,
                                         'IsNullable': NO_NULLS},
                                        {'Name': 'Column',
                                         'TypeName': 'INTEGER',
                                         'Type': 4,
                                         'IsNullable': NO_NULLS},
                                        {'Name': 'ReferencedTable',
                                         'TypeName': 'INTEGER',
                                         'Type': 4,
                                         'IsNullable': NO_NULLS},
                                        {'Name': 'RelatedColumn',
                                         'TypeName': 'INTEGER',
                                         'Type': 4,
                                         'IsNullable': NO_NULLS},
                                        {'Name': 'UpdateRule',
                                         'TypeName': 'INTEGER',
                                         'Type': 4,
                                         'IsNullable': NO_NULLS},
                                        {'Name': 'DeleteRule',
                                         'TypeName': 'INTEGER',
                                         'Type': 4,
                                         'IsNullable': NO_NULLS})},
            'Indexes':     {'CatalogName': 'PUBLIC',
                            'SchemaName':  'PUBLIC',
                            'Type':        'TEXT TABLE',
                            'Columns': ({'Name': 'Index',
                                         'TypeName': 'INTEGER',
                                         'Type': 4,
                                         'IsNullable': NO_NULLS},
                                        {'Name': 'Table',
                                         'TypeName': 'INTEGER',
                                         'Type': 4,
                                         'IsNullable': NO_NULLS},
                                        {'Name': 'Column',
                                         'TypeName': 'INTEGER',
                                         'Type': 4,
                                         'IsNullable': NO_NULLS},
                                        {'Name': 'Unique',
                                         'TypeName': 'BOOLEAN',
                                         'Type': 16,
                                         'IsNullable': NO_NULLS,
                                         'DefaultValue': 'TRUE'})},
            'Privileges':  {'CatalogName': 'PUBLIC',
                            'SchemaName':  'PUBLIC',
                            'Type':        'TEXT TABLE',
                            'Columns': ({'Name': 'Table',
                                         'TypeName': 'INTEGER',
                                         'Type': 4,
                                         'IsNullable': NO_NULLS},
                                        {'Name': 'Column',
                                         'TypeName': 'INTEGER',
                                         'Type': 4,
                                         'IsNullable': NULLABLE,
                                         'DefaultValue': 'NULL'},
                                        {'Name': 'Role',
                                         'TypeName': 'VARCHAR',
                                         'Type': 12,
                                         'Scale': 100,
                                         'IsNullable': NO_NULLS},
                                        {'Name': 'Privilege',
                                         'TypeName': 'INTEGER',
                                         'Type': 4,
                                         'IsNullable': NO_NULLS})},
            'Settings':    {'CatalogName': 'PUBLIC',
                            'SchemaName':  'PUBLIC',
                            'Type':        'TEXT TABLE',
                            'Columns': ({'Name': 'Id',
                                         'TypeName': 'INTEGER',
                                         'Type': 4,
                                         'IsNullable': NO_NULLS},
                                        {'Name': 'Name',
                                         'TypeName': 'VARCHAR',
                                         'Type': 12,
                                         'Scale': 100,
                                         'IsNullable': NO_NULLS},
                                        {'Name': 'Value1',
                                         'TypeName': 'VARCHAR',
                                         'Type': 12,
                                         'Scale': 100,
                                         'IsNullable': NO_NULLS},
                                        {'Name': 'Value2',
                                         'TypeName': 'VARCHAR',
                                         'Type': 12,
                                         'Scale': 100,
                                         'IsNullable': NULLABLE,
                                         'DefaultValue': 'NULL'},
                                        {'Name': 'Value3',
                                         'TypeName': 'VARCHAR',
                                         'Type': 12,
                                         'Scale': 100,
                                         'IsNullable': NULLABLE,
                                         'DefaultValue': 'NULL'}),
                            'PrimaryKeys': ('Id', )}}

