from . import sqlalchemy_hive

import re

from sqlalchemy import exc, util, types
from sqlalchemy.engine import default
from pyhive.sqlalchemy_hive import _type_map


class SparkSqlDialect(sqlalchemy_hive.HiveDialect):
    name = b'sparksql'
    execution_ctx_cls = default.DefaultExecutionContext

    def get_table_comment(self, connection, table_name, schema=None, **kw):
        rows = self._get_table_columns(connection, table_name, schema, extended=True)

        # Remove the column type specs.
        start_detailed_info_index = rows.index(('# Detailed Table Information', '', ''))
        assert start_detailed_info_index >= 0
        rows = rows[start_detailed_info_index:]

        # Generate properties dictionary.
        properties = {}
        active_heading = None
        for col_name, data_type, value in rows:
            col_name: str = col_name.rstrip()
            if col_name.startswith('# '):
                continue
            elif col_name == "" and data_type is None:
                active_heading = None
                continue
            elif col_name != "" and data_type is None:
                active_heading = col_name
            elif col_name != "" and data_type is not None:
                properties[col_name] = data_type.strip()
            else:
                # col_name == "", data_type is not None
                prop_name = "{} {}".format(active_heading, data_type.rstrip())
                properties[prop_name] = value.rstrip()

        return {'text': properties.get('Table Parameters: comment', None), 'properties': properties}

    def get_columns(self, connection, table_name, schema=None, **kw):
        rows = self._get_table_columns(connection, table_name, schema)
        # Strip whitespace
        rows = [[col.strip() if col else None for col in row] for row in rows]
        # Filter out empty rows and comment
        rows = [row for row in rows if row[0] and row[0] != '# col_name']
        result = []
        for (col_name, full_col_type, comment) in rows:
            if col_name == '# Partition Information' or col_name == '# Partitioning':
                break
            # Take out the more detailed type information
            # e.g. 'map<int,int>' -> 'map'
            #      'decimal(10,1)' -> decimal
            col_type = re.search(r'^\w+', full_col_type).group(0)
            try:
                coltype = _type_map[col_type]
            except KeyError:
                util.warn("Did not recognize type '%s' of column '%s'" % (col_type, col_name))
                coltype = types.NullType

            result.append({
                'name': col_name,
                'type': coltype,
                'full_type': full_col_type,
                'nullable': True,
                'default': None,
                'comment': comment,
            })
        return result

    def _get_table_columns(self, connection, table_name, schema, extended=False):
        full_table = table_name
        if schema:
            full_table = schema + '.' + table_name
        # TODO using TGetColumnsReq hangs after sending TFetchResultsReq.
        # Using DESCRIBE works but is uglier.
        try:
            # This needs the table name to be unescaped (no backticks).
            extended = " FORMATTED" if extended else ""
            rows = connection.execute('DESCRIBE{} {}'.format(extended, full_table)).fetchall()
        except exc.OperationalError as e:
            # Does the table exist?
            regex_fmt = r'TExecuteStatementResp.*NoSuchTableException.*Table or view \'{}\'' \
                        r' not found'
            regex = regex_fmt.format(re.escape(table_name))
            if re.search(regex, e.args[0]):
                raise exc.NoSuchTableError(full_table)
            elif schema:
                schema_regex_fmt = r'TExecuteStatementResp.*NoSuchDatabaseException.*Database ' \
                                   r'\'{}\' not found'
                schema_regex = schema_regex_fmt.format(re.escape(schema))
                if re.search(schema_regex, e.args[0]):
                    raise exc.NoSuchTableError(full_table)
            else:
                # When a hive-only column exists in a table
                hive_regex_fmt = r'org.apache.spark.SparkException: Cannot recognize hive type ' \
                                 r'string'
                if re.search(hive_regex_fmt, e.args[0]):
                    raise exc.UnreflectableTableError
                else:
                    raise
        else:
            return rows

    def get_table_names(self, connection, schema=None, **kw):
        query = 'SHOW TABLES'
        if schema:
            query += ' IN ' + self.identifier_preparer.quote_identifier(schema)
        return list(row[1] for row in filter(
            lambda x: not x[-1],
            [row for row in connection.execute(query)]
        ))

    def get_view_names(self, connection, schema=None, **kw):
        query = 'SHOW TABLES'
        if schema:
            query += ' IN ' + self.identifier_preparer.quote_identifier(schema)
        return list(row[1] for row in filter(
            lambda x: x[-1],
            [row for row in connection.execute(query)]
        ))

    def has_table(self, connection, table_name, schema=None):
        try:
            self._get_table_columns(connection, table_name, schema)
            return True
        except exc.NoSuchTableError:
            return False
        except exc.UnreflectableTableError:
            return False