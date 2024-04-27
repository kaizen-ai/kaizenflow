from flask import Flask, render_template, request, redirect, url_for
from models import get_posts, get_post
from flask import abort
from flask import request, redirect, url_for
from models import add_comment as model_add_comment
from models import create_post as model_create_post
from models import delete_post as model_delete_post
from models import create_post_counter_table
from flask import request


app = Flask(__name__)

@app.route('/')
@app.route('/index')
def index():
    posts = get_posts()
    return render_template('home.html', posts=posts)

@app.route('/create_post', methods=['GET', 'POST'])
def create_post():
    if request.method == 'POST':
        title = request.form['title']
        content = request.form['content']
        model_create_post(title, content)
        return redirect(url_for('index'))
    return render_template('create_post.html')


@app.route('/post/<int:post_id>')
def post(post_id):
    post = get_post(post_id)
    if post is None:
        abort(404)
    return render_template('post.html', post=post)

@app.route('/post/<int:post_id>/add_comment', methods=['POST'])
def add_comment(post_id):
    commenter = request.form['commenter']
    comment = request.form['comment']
    model_add_comment(post_id, commenter, comment)
    return redirect(url_for('post', post_id=post_id))

@app.route('/post/<int:post_id>/delete', methods=['GET'])
def delete_post(post_id):
    model_delete_post(post_id)
    return redirect(url_for('index'))

if __name__ == "__main__":
    # create_post_counter_table()
    app.run(host='0.0.0.0', port=8080)

