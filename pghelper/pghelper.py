from collections import namedtuple
from enum import Enum
from uuid import uuid4

import psycopg2
from psycopg2.extensions import AsIs, cursor


class Format(Enum):
    """
    Enum for output format of PG
    """

    Dict = 1
    NamedTuple = 2


class PG(psycopg2.extensions.connection):
    """
    Extension of the psycopg2 connection class
    """

    def __init__(self, output_format=Format.NamedTuple, **kwargs):
        """
        Parameters
        ----------
        output_format: Format
            format of rows returned by query
        """
        self.format = output_format
        dsn = psycopg2.extensions.make_dsn(**kwargs)
        super(PG, self).__init__(dsn)

    def get_columns(self, cursor: cursor, table: str):
        """
        Get column name for table
        Parameters
        ----------
        cursor: psycopg2.extensions.cursor
            psycopg2 cursor
        table: str
            SQL table

        Returns
        -------
        list
            a list of str representing the table columns.
        """
        cursor.execute(
            f"""select column_name from information_schema.columns where table_name='{table}'"""
        )
        return [a[0] for a in cursor.fetchall()]

    def _format_row(self, rows: list, columns: list):
        """
        Format output rows according to the selected output format
        Parameters
        ----------
        rows: list
            data returned by psycopg2 cursor execute function
        columns: list
            a list of string representing columns associated with rows

        Returns
        -------
        list of dict or tuple
            a list of dictionary or tuple representing the rows.
        """
        if self.format == Format.Dict:
            return [dict(zip(columns, r)) for r in rows]
        elif self.format == Format.NamedTuple:
            Table = namedtuple("Table", columns)
            return [Table(*r) for r in rows]

    def _select_stmt(self, cursor, columns, table, condition=""):
        existing_columns = self.get_columns(cursor, table)
        if columns == "*":
            columns = existing_columns
        elif isinstance(columns, str):
            columns = columns.replace(" ", "")
            columns = columns.split(",")
        else:
            columns = list(filter(lambda x: x in existing_columns, columns))
        query_col = ",".join(columns)
        query = f"select {query_col} from {table} {condition};"
        return query, columns

    def select_query(self, columns: str, table: str, condition=""):
        """
        Database select query
        'Select [columns] from [table] [condition]'
        Parameters
        ----------
        columns: list
            columns selected in SQL statement
        table: list
            SQL table
        condition: str
            condition clause at the end of SQL statement (i.e WHERE, ORDER)

        Returns
        -------
        list of dict or tuple
            a list of dictionary or tuple representing the rows.
        """
        cursor = self.cursor()
        query, columns = self._select_stmt(cursor, columns, table, condition)
        cursor.execute(query)
        return self._format_row(cursor.fetchall(), columns)

    def safe_insert(self, table, data, return_insert=False, commit=False):
        """
        Insert row as dictionary
        Parameters
        ----------
        table: str
            SQL table
        data: dict
            dictionary representing sql row to insert, dict keys should be the same as the sql table's columns
        return_insert: bool
            return the inserted row
        commit: bool
            commit insert to database

        Returns
        -------
        list of dict or tuple
            a list of dictionary or tuple representing the inserted rows.
        """
        rv = None
        cursor = self.cursor()
        columns = self.get_columns(cursor, table)
        filtered_data = {k: v for k, v in data.items() if k in columns}

        if return_insert:
            insert_stmt = f"""INSERT INTO {table} (%s) VALUES %s RETURNING %s;"""
            sub = (
                AsIs(",".join(filtered_data.keys())),
                tuple(filtered_data.values()),
                AsIs(",".join(columns)),
            )
        else:
            insert_stmt = f"""INSERT INTO {table} (%s) VALUES %s;"""
            sub = (AsIs(",".join(filtered_data.keys())), tuple(filtered_data.values()))

        stmt = cursor.mogrify(insert_stmt, sub)
        cursor.execute(stmt)

        if return_insert:
            rv = self._format_row(cursor.fetchall(), columns)
        cursor.close()

        if commit:
            self.commit()
        return rv

    def safe_insert_bulk(self, table, data, return_insert=True, commit=False):
        """
        Insert rows as list of dictionaries

        Parameters
        ----------
        table: str
            SQL table
        data: list[dict]
            dictionary representing sql row to insert, dict keys should be the same as the sql table's columns
        return_insert: bool
            return the inserted rows
        commit: bool
            commit inserts to database

        Returns
        -------
        list of dict or tuple
            a list of dictionary or tuple representing the inserted rows.
        """
        if not data:
            raise Exception("No Data to insert")
        rv = None
        cursor = self.cursor()
        columns = self.get_columns(cursor, table)
        subs = []
        row = ""

        for d in data:
            filtered_data = {k: v for k, v in d.items() if k in columns}
            subs.append(tuple(filtered_data.values()))
            row += f",%s"

        if len(row):
            row = row[1:]
        subs.insert(0, AsIs(",".join(filtered_data.keys())))

        if return_insert:
            subs.append(AsIs(",".join(columns)))
            insert_stmt = f"""INSERT INTO {table} (%s) VALUES {row} RETURNING %s;"""
        else:
            insert_stmt = f"""INSERT INTO {table} (%s) VALUES {row};"""

        stmt = cursor.mogrify(insert_stmt, tuple(subs))
        cursor.execute(stmt)

        if return_insert:
            rv = self._format_row(cursor.fetchall(), columns)
        cursor.close()

        if commit:
            self.commit()
        return rv

    def safe_update(self, table, data, condition="", table_update=False, commit=False):
        """
        Update statement

        Parameters
        ----------
        table: str
            SQL table
        data: dict
            dictionary representing the sql table's columns that should be updated
        condition: str
            WHERE Clause
        table_update: bool
            allow update without WHERE clause (entire table)
        commit: bool
            commit update to database
        """
        if not condition and not table_update:
            raise Exception("Use a where close in the statement")
        cursor = self.cursor()
        columns = self.get_columns(cursor, table)
        existing = self.select_query(columns, table, condition)
        if len(existing) == 0:
            raise Exception("No row match condition for update.")

        col_update = ",".join(
            [
                cursor.mogrify(f"{k}=%s", (v,)).decode("utf-8")
                for k, v in data.items()
                if k in columns
            ]
        )
        update_stmt = f"UPDATE {table} SET {col_update} {condition};"
        cursor.execute(update_stmt)
        cursor.close()
        if commit:
            self.commit()

    def stream_select_query(self, batch: int, columns: str, table: str, condition=""):
        """
        Database select query with server size pagination
        'Select [columns] from [table] [condition]'
        Parameters
        ----------
        batch_size: int
            server side pagination batch size to save on memory
        columns: list
            columns selected in SQL statement
        table: list
            SQL table
        condition: str
            condition clause at the end of SQL statement (i.e WHERE, ORDER)

        Returns
        -------
        list of dict or tuple
            a list of dictionary or tuple representing the rows.
        """
        cursor = self.cursor()
        cursor.itersize = batch_size
        query, columns = self._select_stmt(cursor, columns, table, condition)
        cursor = self.cursor(str(uuid4()))
        cursor.execute(query)
        for row in cursor:
            yield self._format_row([row], columns)[0]
