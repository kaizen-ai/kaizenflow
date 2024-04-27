from rethinkdb import RethinkDB
r = RethinkDB()

conn = r.connect("rethinkdb", 28015, db="blog").repl()


if "blog" not in r.db_list().run(conn):
    # Create a new database called "blog"
    r.db_create("blog").run(conn)

    # Create tables for posts and comments
    r.db("blog").table_create("posts").run(conn)


def get_next_post_id():
    current_id = r.table("post_counter").get("post_id").run(conn)
    if current_id["value"] == 0:
        return 1
    return current_id["value"] + 1


def create_post(title, content):
    post_id = get_next_post_id()
    post = {
        "id": post_id,
        "title": title,
        "content": content,
        "comments": []
    }
    r.table("posts").insert(post).run(conn)
    r.table("post_counter").get("post_id").update({"value": post_id}).run(conn)

def get_posts():
    return list(r.table("posts").run(conn))

def get_post(post_id):
    return r.table("posts").get(post_id).run(conn)

def add_comment(post_id, commenter, comment):
    comment_data = {
        "commenter": commenter,
        "comment": comment
    }
    r.table("posts").get(post_id).update({
        "comments": r.row["comments"].append(comment_data)
    }).run(conn)

def get_comments(post_id):
    post = r.table("posts").get(post_id).run(conn)
    if post:
        comments = post.get("comments", [])
        if comments:
            print("Comments for Post ID", post_id)
            for comment in comments:
                print("Commenter:", comment["commenter"])
                print("Comment:", comment["comment"])
                print("-" * 30)
        else:
            print("No comments found for Post ID", post_id)
    else:
        print("Post with ID", post_id, "not found")

def delete_comment(post_id, comment_id):
    r.table("posts").get(post_id).update({
        "comments": r.row["comments"].filter(lambda comment:
            comment["id"] != comment_id)
    }).run(conn)

def delete_post(post_id):
    # Delete the post from the 'posts' table
    result = r.table("posts").get(post_id).delete().run(conn)
    
    # If the post was successfully deleted, update the post counter if needed
    if result['deleted'] > 0:
        # Get the current value of the post counter
        current_id = r.table("post_counter").get("post_id").run(conn)
        
        # Update the post counter if the deleted post had the highest ID
        if current_id["value"] == post_id:
            # Find the highest ID among remaining posts
            highest_id = r.table("posts").max("id").run(conn)
            new_highest_id = highest_id["id"] if highest_id else 0
            
            # Update the post counter with the new highest ID
            r.table("post_counter").get("post_id").update({"value": new_highest_id}).run(conn)
        
        print(f"Post with ID {post_id} deleted successfully.")
    else:
        print(f"Post with ID {post_id} not found.")

def create_post_counter_table():
    r.table_create("post_counter").run(conn)
    r.table("post_counter").insert({"id": "post_id", "value": 0}).run(conn)
