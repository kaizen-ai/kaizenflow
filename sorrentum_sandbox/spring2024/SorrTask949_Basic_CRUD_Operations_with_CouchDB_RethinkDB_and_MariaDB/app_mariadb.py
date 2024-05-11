from flask import Flask, Blueprint, render_template, request, redirect, url_for
import mysql.connector
from flask import Blueprint

app = Flask(__name__)

# MariaDB configuration
config = {
    'user': 'root',
    'password': '1971',
    'host': 'localhost',
    'database': 'students_mariadb',
    'raise_on_warnings': True
}

# Connect to MariaDB
conn = mysql.connector.connect(**config)
cursor = conn.cursor(dictionary=True)

# Connect to MariaDB
conn = mysql.connector.connect(**config)
cursor = conn.cursor(dictionary=True)

# Reconnect with the database
config['database'] = 'students_mariadb'
conn = mysql.connector.connect(**config)
cursor = conn.cursor(dictionary=True)




@app.route('/')
def index_mariadb():
    cursor.execute("SELECT * FROM students")
    students = cursor.fetchall()
    return render_template('index_mariadb.html', students=students)

@app.route('/add_mariadb', methods=['GET'])
def add_student_mariadb():
    return render_template('add_student_mariadb.html')

@app.route('/create_mariadb', methods=['POST'])
def create_mariadb():
    name = request.form['name']
    age = request.form['age']
    query = "INSERT INTO students (name, age) VALUES (%s, %s)"
    cursor.execute(query, (name, age))
    conn.commit()
    return redirect(url_for('index_mariadb'))

@app.route('/update_mariadb/<int:student_id>', methods=['GET', 'POST'])
def update_mariadb(student_id):
    if request.method == 'POST':
        name = request.form['name']
        age = request.form['age']
        query = "UPDATE students SET name = %s, age = %s WHERE id = %s"
        cursor.execute(query, (name, age, student_id))
        conn.commit()
        return redirect(url_for('index_mariadb'))
    else:
        query = "SELECT * FROM students WHERE id = %s"
        cursor.execute(query, (student_id,))
        student = cursor.fetchone()
        return render_template('update_student_mariadb.html', student=student, student_id=student_id)

@app.route('/delete_mariadb/<int:student_id>')
def delete_mariadb(student_id):
    query = "DELETE FROM students WHERE id = %s"
    cursor.execute(query, (student_id,))
    conn.commit()
    return redirect(url_for('index_mariadb'))

if __name__ == '__main__':
    app.run(debug=True)
