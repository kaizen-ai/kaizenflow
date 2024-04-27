from rethinkdb import RethinkDB
r = RethinkDB()

conn = r.connect("rethinkdb", 28015)

if "blog" not in r.db_list().run(conn):
    r.db_create("blog").run(conn)

    r.db("blog").table_create("posts").run(conn)

conn.close()
