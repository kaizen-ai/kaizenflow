from rethinkdb import RethinkDB
r = RethinkDB()

# Connect to RethinkDB
conn = r.connect("rethinkdb", 28015)

# Check if the "blog" database exists
if "blog" not in r.db_list().run(conn):
    # Create a new database called "blog"
    r.db_create("blog").run(conn)

    # Create tables for posts and comments
    r.db("blog").table_create("posts").run(conn)

# Close the connection
conn.close()
