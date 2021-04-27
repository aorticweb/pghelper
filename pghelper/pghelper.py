from collections import namedtuple
from enum import Enum

import psycopg2
from psycopg2.extensions import AsIs, cursor

class Format(Enum):
    """
    Enum for output format of PG
    """
    Dict = 1
    NamedTuple = 2

class PG(psycopg2.extensions.connection):
    def __init__(self, output_format=Format.NamedTuple, **kwargs):
        self.format = output_format
        del kwargs["output_format"]
        dsn = psycopg2.extensions.make_dsn(**kwargs)
        super(PG, self).__init__(dsn)

    def get_columns(self, cursor :cursor, table :str):
        """
        Get column name for table
        """
        cursor.execute(
            f"""select column_name from information_schema.columns where table_name='{table}'"""
        )
        return [a[0] for a in cursor.fetchall()]

    def _format_row(self, rows :list, columns :list):
        """
        Format output rows according to the selected output format
        """
        if self.format == Format.Dict:
            return [dict(zip(columns, r)) for r in rows]
        elif self.format == Format.NamedTuple:
            Table = namedtuple("Table", columns)
            return [Table(*r) for r in rows]

    def select_query(self, columns, table, condition=""):
        """
        """
        cursor = self.cursor()
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
        cursor.execute(query)
        return self._format_row(cursor.fetchall(), columns)

    def safe_insert(self, table, data, return_insert=False, commit=False):
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
        TODO
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
        # print(cursor.query)
        cursor.close()
        if commit:
            self.commit()



