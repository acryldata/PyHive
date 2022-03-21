import re

from . import sqlalchemy_hive


from sqlalchemy import exc
from sqlalchemy.engine import default


class SparkSqlDialect(sqlalchemy_hive.HiveDialect):
    name = b'sparksql'
    execution_ctx_cls = default.DefaultExecutionContext
    partition_columns_names = ['# Partition Information', '# Partitioning']

    def _get_table_columns(self, connection, table_name, schema, extended=False):
        full_table = table_name
        if schema:
            full_table = schema + '.' + table_name
        # TODO using TGetColumnsReq hangs after sending TFetchResultsReq.
        # Using DESCRIBE works but is uglier.
        try:
            # we need to set this to avoid sparksql truncating long column types (i.e. structs), arbitrarily chose 1kk
            connection.execute("SET spark.sql.debug.maxToStringFields=1000000")
            extended = " FORMATTED" if extended else ""
            rows = connection.execute('DESCRIBE{} {}'.format(extended, full_table)).fetchall()
        except exc.OperationalError as e:
            regex_fmt = r'TExecuteStatementResp.*AnalysisException.*Table or view not found:.*{}'
            hive_regex_fmt = r'org.apache.spark.SparkException: Cannot recognize hive type ' \
                             r'string'
            if re.search(regex_fmt.format(re.escape(table_name)), e.args[0]):
                raise exc.NoSuchTableError(full_table)
            elif re.search(hive_regex_fmt, e.args[0]):
                raise exc.UnreflectableTableError
            else:
                raise
        else:
            return rows

    def get_table_names(self, connection, schema=None, **kw):
        # SHOW TABLES will show tables and views, SHOW VIEWS only views, if it is needed we could potentially
        # subtract set of views from set of tables to get a proper list of only tables
        # since hive dialect implementation does not support views extraction get_view_names need to be reimplemented
        # too
        query = 'SHOW TABLES'
        if schema:
            query += ' IN ' + self.identifier_preparer.quote_identifier(schema)
        # returns tuples ('database', 'tableName', 'isTemporary')
        result = connection.execute(query)
        a = [row[1] for row in result if not row[-1]]
        return a

    def has_table(self, connection, table_name, schema=None):
        try:
            return super().has_table(connection, table_name, schema)
        except exc.UnreflectableTableError:
            return False
