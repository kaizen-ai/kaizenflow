from flask import Flask, Blueprint, render_template, request, redirect, url_for
import rethinkdb as r


app = Flask(__name__)

# RethinkDB configuration
RDB_HOST = 'localhost'
RDB_PORT = 28015
DB_NAME = 'students_rethinkdb'

import rethinkdb as r

# Connection details for RethinkDB
r.r.connect('localhost', 28015).repl()

# Check if database exists
db_list = r.r.db_list().run()
if 'students_rethinkdb' not in db_list:
    r.r.db_create('students_rethinkdb').run()

# Connect to the database
r_conn = r.r.connect('localhost', 28015, db='students_rethinkdb').repl()

# Check if table exists
table_list = r.r.db('students_rethinkdb').table_list().run()
if 'students' not in table_list:
    r.r.db('students_rethinkdb').table_create('students').run()


@app.route('/')
def index_rethinkdb():
    students = list(r.r.db(DB_NAME).table('students').run(r_conn))
    return render_template('index_rethinkdb.html', students=students)

@app.route('/add_rethinkdb', methods=['GET'])
def add_student_rethinkdb():
    return render_template('add_student_rethinkdb.html')

@app.route('/create_rethinkdb', methods=['POST'])
def create_rethinkdb():
    student = {
        'name': request.form['name'],
        'age': request.form['age']
    }
    r.r.db(DB_NAME).table('students').insert(student).run(r_conn)
    return redirect(url_for('index_rethinkdb'))

@app.route('/update_rethinkdb/<doc_id>', methods=['GET', 'POST'])
def update_rethinkdb(doc_id):
    student = r.r.db(DB_NAME).table('students').get(doc_id).run(r_conn)
    if request.method == 'POST':
        update_data = {
            'name': request.form['name'],
            'age': request.form['age']
        }
        r.r.db(DB_NAME).table('students').get(doc_id).update(update_data).run(r_conn)
        return redirect(url_for('index_rethinkdb'))
    return render_template('update_student_rethinkdb.html', student=student, doc_id=doc_id)

@app.route('/delete_rethinkdb/<doc_id>')
def delete_rethinkdb(doc_id):
    r.r.db(DB_NAME).table('students').get(doc_id).delete().run(r_conn)
    return redirect(url_for('index_rethinkdb'))

if __name__ == '__main__':
    app.run(debug=True)
