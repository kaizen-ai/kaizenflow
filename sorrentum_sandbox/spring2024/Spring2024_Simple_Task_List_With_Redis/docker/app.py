from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash
import sys
import logging
from flask import Flask, request, jsonify, g, session
from redis import Redis
import json
import secrets
import hashlib
import uuid


app = Flask(__name__)
# Connect to Redis
r = Redis(host='redis', port=6379)

# Endpoint for user registration
@app.route('/register', methods=['POST'])
def register():
    data = request.json
    username = data.get('username')
    email = data.get('email')
    password = data.get('password')
    
    # Check if username or email is already in use
    if r.exists(f'user:username:{username}'):
        return jsonify({'error': 'Username already exists'}), 400
    if r.exists(f'user:email:{email}'):
        return jsonify({'error': 'Email already exists'}), 400
    
    # Hash the password
    hashed_password = generate_password_hash(password)
    
    # Generate user ID
    user_id = secrets.token_hex(16)
    
    # Create user profile
    profile = {
        'username': username,
        'email': email,
        'password': hashed_password,
        'name': '',
        'bio': ''
    }
    
    r.hmset(f'user:{user_id}', profile)
    r.set(f'user:username:{username}', user_id)
    r.set(f'user:email:{email}', user_id)
    
    return jsonify({'message': 'User registered successfully', 'user_id': user_id}), 201

# Endpoint for user authentication
@app.route('/login', methods=['POST'])
def login():
    data = request.json
    username = data.get('username')
    password = data.get('password')
    
    # Find the user by username
    user_id = r.get(f'user:username:{username}')
    if user_id:
        user_data = r.hgetall(f'user:{user_id.decode()}')
        if check_password_hash(user_data[b'password'].decode(), password):
            return jsonify({'message': 'Login successful', 'user_id': user_id.decode()}), 200
    
    return jsonify({'error': 'Invalid username or password'}), 401


# Endpoint to create a new task
@app.route('/tasks', methods=['POST'])
def create_task():
    task_data = request.json
    task_id = r.incr('task:id')
    task_data['id'] = task_id
    r.set(f'task:{task_id}', json.dumps(task_data))
    return jsonify(task_data), 201

# Endpoint to get all tasks
@app.route('/tasks', methods=['GET'])
def get_tasks():
    tasks = []
    task_ids = r.keys('task:*')
    for task_id in task_ids:
        task_data = json.loads(r.get(task_id))
        tasks.append(task_data)
    return jsonify(tasks)

# Endpoint to get a single task by ID
@app.route('/tasks/<int:task_id>', methods=['GET'])
def get_task(task_id):
    task_data = r.get(f'task:{task_id}')
    if task_data is None:
        return jsonify({'error': 'Task not found'}), 404
    return jsonify(json.loads(task_data))

# Endpoint to update a task by ID
@app.route('/tasks/<int:task_id>', methods=['PUT'])
def update_task(task_id):
    task_data = request.json
    task_data['id'] = task_id
    if r.exists(f'task:{task_id}'):
        r.set(f'task:{task_id}', json.dumps(task_data))
        return jsonify(task_data)
    else:
        return jsonify({'error': 'Task not found'}), 404

# Endpoint to delete a task by ID
@app.route('/tasks/<int:task_id>', methods=['DELETE'])
def delete_task(task_id):
    if r.exists(f'task:{task_id}'):
        r.delete(f'task:{task_id}')
        return jsonify({'message': 'Task deleted'}), 200
    else:
        return jsonify({'error': 'Task not found'}), 404

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
