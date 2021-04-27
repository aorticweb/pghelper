import os
from collections import namedtuple
from uuid import uuid4

from pghelper import PG, Format


def db(format=Format.NamedTuple):
    host = os.environ["PGHOST"]
    port = os.environ["PGPORT"]
    user = os.environ["PGUSER"]
    password = os.environ["PGPASSWORD"]
    return PG(
        output_format=format,
        dbname="postgres",
        user=user,
        password=password,
        host=host,
        port=port,
    )


def clear_table(conn: PG):
    cursor = conn.cursor()
    cursor.execute("DELETE FROM orders where 1=1;")
    cursor.close()
    conn.commit()


def insert_data(conn: PG, count=0):
    data = []
    for c in range(0, count):
        d = {"item_name": "spoon", "price": (50 + c), "identifier": str(uuid4())}
        data.append(d)
    return conn.safe_insert_bulk("orders", data, True, True)


def test_get_columns():
    conn = db()
    cursor = conn.cursor()
    columns = conn.get_columns(cursor, "orders")
    assert len(columns) > 0


def test_select_query():
    conn = db()
    insert_data(conn, 5)
    data = conn.select_query("*", "orders")
    assert len(data) == 5
    assert isinstance(data[0], tuple)
    conn = db(Format.Dict)
    data = conn.select_query("*", "orders")
    assert len(data) == 5
    assert isinstance(data[0], dict)
    data = conn.select_query("*", "orders", "WHERE price <= 50")
    assert len(data) == 1
    data = conn.select_query("identifier", "orders")
    assert data[0].get("id") is None
    clear_table(conn)


def test_safe_insert():
    conn = db(format=Format.Dict)
    i = str(uuid4())
    data = {
        "item_name": "spoon",
        "price": 50,
        "client_name": "aorticweb",
        "identifier": i,
    }
    insert_rows = conn.safe_insert("orders", data, return_insert=True, commit=True)
    rows = conn.select_query("*", "orders", f"WHERE identifier='{i}'")
    assert len(rows) == 1
    for k in data.keys():
        assert data[k] == rows[0][k]
        assert data[k] == insert_rows[0][k]
    clear_table(conn)


def test_safe_insert_bulk():
    conn = db(format=Format.Dict)
    data = []
    count = 10
    for c in range(0, count):
        d = {"item_name": "spoon", "price": (50 + c), "identifier": str(uuid4())}
        data.append(d)
    identifiers = [d["identifier"] for d in data]
    insert_rows = conn.safe_insert_bulk("orders", data, return_insert=True, commit=True)
    rows = conn.select_query("*", "orders")
    assert len(rows) == count
    assert len(insert_rows) == count
    for i in range(0, count):
        assert rows[i]["identifier"] in identifiers
        assert insert_rows[i]["identifier"] in identifiers
    clear_table(conn)

def test_safe_update():
    conn = db(format=Format.Dict)
    insert_data(conn, 5)
    data = conn.select_query("*", "orders")
    conn.safe_update("orders", {"client_name": "aorticweb"}, "where price%2=0", commit=True)
    data = conn.select_query("*", "orders", "where price%2=0")
    for d in data:
        if d["price"] % 2 == 0:
            assert d["client_name"] == "aorticweb"
    clear_table(conn)

def test_stream_select_query():
    conn = db(format=Format.Dict)
    insert_data(conn, 500) 
    data = conn.stream_select_query(100, "*", "orders")
    for d in data:
        assert data["id"]
    clear_table(conn)
