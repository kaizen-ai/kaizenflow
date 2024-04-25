pip install Flask

import json
from flask import Flask, jsonify, request

app = Flask(__name__)

import json
from flask import Flask, jsonify, request
app = Flask(__name__)

employees = [
 { 'id': 1, 'name': 'Ashley' },
 { 'id': 2, 'name': 'Kate' },
 { 'id': 3, 'name': 'Joe' }
]


nextEmployeeId = 4
3
@app.route('/employees', methods=['GET'])
def get_employees():
 return jsonify(employees)

@app.route('/employees/<int:id>', methods=['GET'])
def get_employee_by_id(id: int):
  employee = get_employee(id)
  if employee is None:
    return jsonify({ 'error': 'Employee does not exist'}), 404
  return jsonify(employee)

def get_employee(id):
 return next((e for e in employees if e['id'] == id), None)

def employee_is_valid(employee):
 for key in employee.keys():
   if key != 'name':
     return False
 return True

@app.route('/employees', methods=['POST'])
def create_employee():
 global nextEmployeeId
 employee = json.loads(request.data)
 if not employee_is_valid(employee):
   return jsonify({ 'error': 'Invalid employee properties.' }), 400

 employee['id'] = nextEmployeeId
 nextEmployeeId += 1
 employees.append(employee)

 return '', 201, { 'location': f'/employees/{employee["id"]}' }

@app.route('/employees/<int:id>', methods=['PUT'])
def update_employee(id: int):
 employee = get_employee(id)
 if employee is None:
   return jsonify({ 'error': 'Employee does not exist.' }), 404

 updated_employee = json.loads(request.data)
 if not employee_is_valid(updated_employee):
   return jsonify({ 'error': 'Invalid employee properties.' }), 400

 employee.update(updated_employee)

 return jsonify(employee)

@app.route('/employees/<int:id>', methods=['DELETE'])
def delete_employee(id: int):
 global employees
 employee = get_employee(id)
 if employee is None:
   return jsonify({ 'error': 'Employee does not exist.' }), 404

 employees = [e for e in employees if e['id'] != id]
 return jsonify(employee), 200

if __name__ == '__main__':
   app.run(port=5000)
