from flask import Flask, jsonify, request
from flask_jwt_extended import JWTManager, jwt_required, create_access_token, get_jwt_identity
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Employee, Department, DeptEmployee, DeptManager, Title, Salary
import uuid
import csv
from io import StringIO
import re
import os
from sqlalchemy import create_engine, text
import mysql.connector
import hashlib 

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your_secret_key_here'
jwt = JWTManager(app)

MYSQL_USERNAME = 'username'
MYSQL_PASSWORD = 'password23!'
MYSQL_HOST = 'employee-db-sketl.mysql.database.azure.com'
MYSQL_DATABASE = 'employees'


# Create a SQLAlchemy engine
engine = create_engine(f'mysql+mysqlconnector://{MYSQL_USERNAME}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}')

Session = sessionmaker(bind=engine)
session = Session()

#----------------Authentication-----------------#
# Initialize JWTManager
jwt = JWTManager(app)
users = {
    "username": "password"
}

# Token-based authentication endpoint
@app.route('/auth', methods=['POST'])
def authenticate():
    data = request.json
    username = data.get('username')
    password = data.get('password')

    # Dummy user authentication (replace with your own authentication logic)
    if username in users and users[username] == password:
        # Generate JWT token
        access_token = create_access_token(identity=username)
        return jsonify(access_token=access_token), 200
    else:
        return jsonify({'error': 'Invalid credentials'}), 401

# Protected endpoint that requires a valid JWT token
@app.route('/protected', methods=['GET'])
@jwt_required()
def protected():
    # Access identity of the current user with get_jwt_identity
    current_user = get_jwt_identity()
    return jsonify(logged_in_as=current_user), 200


# Routes for CRUD operations
# ---------------Create Operations----------------#

# Generate a unique ID
def generate_unique_id():
    unique_id = uuid.uuid4()
    hashed_id = hashlib.sha1(str(unique_id).encode()).hexdigest()[:6]
    return int(hashed_id, 16)  # Convert hexadecimal to integer


# Endpoint to generate a unique ID
@app.route('/generate-unique-id', methods=['GET'])
@jwt_required()
def generate_unique_id_endpoint():
    unique_id = generate_unique_id()
    return jsonify({'unique_id': unique_id}), 200

# Create a new employee
@app.route('/employees', methods=['POST'])
@jwt_required()
def create_employee():
    # Parse request data
    data = request.json
    emp_no = generate_unique_id()
    # Create a new Employee instance
    new_employee = Employee(
        emp_no=emp_no,
        birth_date=data['birth_date'],
        first_name=data['first_name'],
        last_name=data['last_name'],
        gender=data['gender'],
        hire_date=data['hire_date']
    )

    # Add the new employee to the session
    session.add(new_employee)

    try:
        # Commit the changes to persist the new employee in the database
        session.commit()
        return jsonify({'message': 'Employee created successfully'}), 201
    except Exception as e:
        # Rollback the session in case of any error
        session.rollback()
        return jsonify({'error': str(e)}), 500

# Create Batch Employees
@app.route('/employees/batch', methods=['POST'])
@jwt_required()
def create_employees_batch():
    data = request.json  # Assuming data is a list of employee objects
    new_employees = []

    for item in data:
        new_employee = Employee(
            emp_no=item['emp_no'],
            birth_date=item['birth_date'],
            first_name=item['first_name'],
            last_name=item['last_name'],
            gender=item['gender'],
            hire_date=item['hire_date']
        )
        new_employees.append(new_employee)

    session.add_all(new_employees)

    try:
        session.commit()
        return jsonify({'message': f'{len(new_employees)} employees created successfully'}), 201
    except Exception as e:
        session.rollback()
        return jsonify({'error': str(e)}), 500
    
# Import csv file to create employees
# Import data from a CSV file
@app.route('/import-data', methods=['POST'])
@jwt_required()
def import_data():
    # Check if a file was provided in the request
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']

    # Check if the file has a valid format (e.g., CSV)
    if file.filename.endswith('.csv'):
        try:
            # Process the CSV file and insert data into the database
            # Example: You can use libraries like pandas to read and process the CSV
            # Here, we're assuming you have a function called process_csv_data
            data = process_csv_data(file)
            
            # Add the data to the database
            session.add_all(data)
            session.commit()

            return jsonify({'message': 'Data imported successfully'}), 200
        except Exception as e:
            session.rollback()
            return jsonify({'error': str(e)}), 500
    else:
        return jsonify({'error': 'Unsupported file format'}), 400


# Endpoint to validate imported CSV data
@app.route('/validate-imported-csv', methods=['POST'])
def validate_imported_csv():
    # Check if a file was provided in the request
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']

    # Validate file format (CSV)
    if file.filename.endswith('.csv'):
        try:
            # Read the CSV file
            csv_data = file.stream.read().decode('utf-8')

            # Perform validation on CSV data
            validation_errors = validate_csv_data(csv_data)

            if validation_errors:
                return jsonify({'errors': validation_errors}), 400
            else:
                return jsonify({'message': 'CSV data validation successful'}), 200
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    else:
        return jsonify({'error': 'Unsupported file format'}), 400

# Validation function for CSV data
def validate_csv_data(csv_data):
    validation_errors = []

    # Read CSV data using the csv module
    csv_reader = csv.DictReader(StringIO(csv_data))

    for row in csv_reader:
        # Perform validation checks on each row
        if not validate_row(row):
            validation_errors.append({'error': 'Validation failed for row', 'data': row})

    return validation_errors

# Validation function for a single row of CSV data
def validate_row(row):
    # Example validation checks for each field in the row
    if 'emp_no' not in row or not row['emp_no'].isdigit():
        return False

    if 'first_name' not in row or not row['first_name']:
        return False

    # Add more validation checks as needed

    return True



#-----------Read Operations----------------#
@app.route('/employees', methods=['GET'])
def get_employees():
    # Query all employees using SQLAlchemy ORM
    employees = session.query(Employee).all()

    # Serialize the employees' data
    serialized_employees = []
    for employee in employees:
        serialized_employee = {
            'emp_no': employee.emp_no,
            'birth_date': employee.birth_date,
            'first_name': employee.first_name,
            'last_name': employee.last_name,
            'gender': employee.gender,
            'hire_date': employee.hire_date
        }
        serialized_employees.append(serialized_employee)

    # Return the JSON response containing the employees' data
    return jsonify(serialized_employees)

    # Return the JSON response containing the employees' data
    return jsonify(serialized_employees)
@app.route('/employees/<int:emp_no>', methods=['GET'])
def get_employee(emp_no):
    employee = session.query(Employee).filter_by(emp_no=emp_no).first()
    if employee:
        return jsonify(employee.serialize())
    else:
        return jsonify({'error': 'Employee not found'}), 404

# Filtering employees by gender
@app.route('/employees/filter', methods=['GET'])
def filter_employees():
    gender = request.args.get('gender')
    employees = session.query(Employee).filter_by(gender=gender).all()
    return jsonify([employee.serialize() for employee in employees])

# Sorting employees by hire date
@app.route('/employees/sort', methods=['GET'])
def sort_employees():
    sort_by = request.args.get('sort_by')
    if sort_by == 'hire_date':
        employees = session.query(Employee).order_by(Employee.hire_date).all()
        return jsonify([employee.serialize() for employee in employees])
    else:
        return jsonify({'error': 'Invalid sorting parameter'}), 400

# Pagination
@app.route('/employees/page', methods=['GET'])
def paginate_employees():
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)
    employees = session.query(Employee).paginate(page, per_page, error_out=False)
    return jsonify({
        'employees': [employee.serialize() for employee in employees.items],
        'total_pages': employees.pages,
        'total_employees': employees.total
    })

# Search employees by name
@app.route('/employees/search', methods=['GET'])
def search_employees():
    keyword = request.args.get('keyword')
    employees = session.query(Employee).filter(Employee.first_name.ilike(f'%{keyword}%') |
                                               Employee.last_name.ilike(f'%{keyword}%')).all()
    return jsonify([employee.serialize() for employee in employees])

# Aggregation - Total number of employees
@app.route('/employees/total', methods=['GET'])
def total_employees():
    total_count = session.query(Employee).count()
    return jsonify({'total_employees': total_count})

#------------Update Operations----------------#

# Update Employee Information Endpoint
@app.route('/employees/<int:emp_no>', methods=['PUT'])
@jwt_required()
def update_employee(emp_no):
    # Parse request data
    data = request.json
    new_first_name = data.get('first_name')
    new_last_name = data.get('last_name')
    new_gender = data.get('gender')
    new_hire_date = data.get('hire_date')

    # Create a session
    session = Session()

    try:
        # Retrieve the employee from the database
        employee = session.query(Employee).filter_by(emp_no=emp_no).first()
        if not employee:
            return jsonify({'error': 'Employee not found'}), 404

        # Update employee information
        if new_first_name:
            employee.first_name = new_first_name
        if new_last_name:
            employee.last_name = new_last_name
        if new_gender:
            employee.gender = new_gender
        if new_hire_date:
            employee.hire_date = new_hire_date

        # Commit the changes to the database
        session.commit()

        # Return the updated employee data
        return jsonify(employee.serialize())
    except Exception as e:
        # Rollback the transaction if an error occurs
        session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        # Close the session
        session.close()

# Change Department Assignment Endpoint
@app.route('/employees/<int:emp_no>/departments', methods=['PUT'])
@jwt_required()
def change_department_assignment(emp_no):
    # Parse request data
    data = request.json
    new_dept_no = data.get('dept_no')
    new_from_date = data.get('from_date')
    new_to_date = data.get('to_date')

    # Create a session
    session = Session()

    try:
        # Retrieve the department assignment from the database
        dept_assignment = session.query(DeptEmployee).filter_by(emp_no=emp_no).first()
        if not dept_assignment:
            return jsonify({'error': 'Department assignment not found'}), 404

        # Update department assignment
        if new_dept_no:
            dept_assignment.dept_no = new_dept_no
        if new_from_date:
            dept_assignment.from_date = new_from_date
        if new_to_date:
            dept_assignment.to_date = new_to_date

        # Commit the changes to the database
        session.commit()

        # Return the updated department assignment data
        return jsonify(dept_assignment.serialize())
    except Exception as e:
        # Rollback the transaction if an error occurs
        session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        # Close the session
        session.close()
# Update Employee Title Endpoint
@app.route('/employees/<int:emp_no>/title', methods=['PUT'])
@jwt_required()
def update_employee_title(emp_no):
    # Parse request data
    data = request.json
    new_title = data.get('title')
    new_from_date = data.get('from_date')
    new_to_date = data.get('to_date')

    # Create a session
    session = Session()

    try:
        # Retrieve the title record from the database
        title_record = session.query(Title).filter_by(emp_no=emp_no).first()
        if not title_record:
            return jsonify({'error': 'Title record not found'}), 404

        # Update title information
        if new_title:
            title_record.title = new_title
        if new_from_date:
            title_record.from_date = new_from_date
        if new_to_date:
            title_record.to_date = new_to_date

        # Commit the changes to the database
        session.commit()

        # Return the updated title record data
        return jsonify(title_record.serialize())
    except Exception as e:
        # Rollback the transaction if an error occurs
        session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        # Close the session
        session.close()

# Adjust Employee Salary Endpoint
@app.route('/employees/<int:emp_no>/salary', methods=['PUT'])
@jwt_required()
def adjust_employee_salary(emp_no):
    # Parse request data
    data = request.json
    new_salary = data.get('salary')
    new_from_date = data.get('from_date')
    new_to_date = data.get('to_date')

    # Create a session
    session = Session()

    try:
        # Retrieve the salary record from the database
        salary_record = session.query(Salary).filter_by(emp_no=emp_no).first()
        if not salary_record:
            return jsonify({'error': 'Salary record not found'}), 404

        # Update salary information
        if new_salary:
            salary_record.salary = new_salary
        if new_from_date:
            salary_record.from_date = new_from_date
        if new_to_date:
            salary_record.to_date = new_to_date

        # Commit the changes to the database
        session.commit()

        # Return the updated salary record data
        return jsonify(salary_record.serialize())
    except Exception as e:
        # Rollback the transaction if an error occurs
        session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        # Close the session
        session.close()

# Extend Managerial Assignment Endpoint
@app.route('/employees/<int:emp_no>/managerial_assignment', methods=['PUT'])
@jwt_required()
def extend_managerial_assignment(emp_no):
    # Parse request data
    data = request.json
    new_to_date = data.get('new_to_date')

    # Create a session
    session = Session()

    try:
        # Retrieve the managerial assignment record from the database
        managerial_assignment_record = session.query(DeptManager).filter_by(emp_no=emp_no).first()
        if not managerial_assignment_record:
            return jsonify({'error': 'Managerial assignment record not found'}), 404

        # Update to_date with the new value
        if new_to_date:
            # Convert the new_to_date string to a datetime object
            new_to_date = datetime.strptime(new_to_date, '%Y-%m-%d')
            managerial_assignment_record.to_date = new_to_date

            # Commit the changes to the database
            session.commit()

            # Return the updated managerial assignment record data
            return jsonify(managerial_assignment_record.serialize())
        else:
            return jsonify({'error': 'New to_date is required'}), 400
    except Exception as e:
        # Rollback the transaction if an error occurs
        session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        # Close the session
        session.close()
# Update Department Information Endpoint
@app.route('/departments/<string:dept_no>', methods=['PUT'])
@jwt_required()
def update_department(dept_no):
    # Parse request data
    data = request.json
    new_dept_name = data.get('dept_name')

    # Create a session
    session = Session()

    try:
        # Retrieve the department record from the database
        department = session.query(Department).filter_by(dept_no=dept_no).first()
        if not department:
            return jsonify({'error': 'Department not found'}), 404

        # Update department information
        if new_dept_name:
            department.dept_name = new_dept_name

            # Commit the changes to the database
            session.commit()

            # Return the updated department record data
            return jsonify(department.serialize())
        else:
            return jsonify({'error': 'New department name is required'}), 400
    except Exception as e:
        # Rollback the transaction if an error occurs
        session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        # Close the session
        session.close()

# Modify Title Names Endpoint
# Modify Title Names Endpoint
@app.route('/titles/<int:emp_no>', methods=['PUT'])
@jwt_required()
def modify_title_names(emp_no):
    # Parse request data
    data = request.json
    new_title = data.get('title')

    # Create a session
    session = Session()

    try:
        # Retrieve the title records from the database
        titles = session.query(Title).filter_by(emp_no=emp_no).all()
        if not titles:
            return jsonify({'error': 'No titles found for employee'}), 404

        # Update title names
        if new_title:
            for title in titles:
                title.title = new_title

            # Commit the changes to the database
            session.commit()

            # Return the updated title records data
            return jsonify([title.serialize() for title in titles])
        else:
            return jsonify({'error': 'New title is required'}), 400
    except Exception as e:
        # Rollback the transaction if an error occurs
        session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        # Close the session
        session.close()

# Adjust Salary Ranges Endpoint
@app.route('/salaries/<int:emp_no>', methods=['PUT'])
@jwt_required()
def adjust_salary_ranges(emp_no):
    # Parse request data
    data = request.json
    new_salary = data.get('salary')

    # Create a session
    session = Session()

    try:
        # Retrieve the salary records from the database
        salaries = session.query(Salary).filter_by(emp_no=emp_no).all()
        if not salaries:
            return jsonify({'error': 'No salaries found for employee'}), 404

        # Adjust salary ranges
        if new_salary:
            for salary in salaries:
                salary.salary = new_salary

            # Commit the changes to the database
            session.commit()

            # Return the adjusted salary records data
            return jsonify([salary.serialize() for salary in salaries])
        else:
            return jsonify({'error': 'New salary is required'}), 400
    except Exception as e:
        # Rollback the transaction if an error occurs
        session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        # Close the session
        session.close()

# Update Department Manager Endpoint
@app.route('/dept_managers/<int:emp_no>', methods=['PUT'])
@jwt_required()
def update_department_manager(emp_no):
    # Parse request data
    data = request.json
    new_dept_no = data.get('dept_no')
    new_from_date = data.get('from_date')
    new_to_date = data.get('to_date')

    # Create a session
    session = Session()

    try:
        # Retrieve the department manager record from the database
        dept_manager = session.query(DeptManager).filter_by(emp_no=emp_no).first()
        if not dept_manager:
            return jsonify({'error': 'Department manager not found'}), 404

        # Update department manager information
        if new_dept_no:
            dept_manager.dept_no = new_dept_no
        if new_from_date:
            dept_manager.from_date = new_from_date
        if new_to_date:
            dept_manager.to_date = new_to_date

        # Commit the changes to the database
        session.commit()

        # Return the updated department manager record data
        return jsonify(dept_manager.serialize())
    except Exception as e:
        # Rollback the transaction if an error occurs
        session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        # Close the session
        session.close()


#------------Delete Operations----------------#

# Delete Employee Endpoint
@app.route('/employees/<int:emp_no>', methods=['DELETE'])
@jwt_required()
def delete_employee(emp_no):
    # Create a session
    session = Session()

    try:
        # Retrieve the employee record to be deleted
        employee = session.query(Employee).filter_by(emp_no=emp_no).first()
        if not employee:
            return jsonify({'error': 'Employee not found'}), 404

        # Delete the employee record
        session.delete(employee)

        # Commit the changes to the database
        session.commit()

        return jsonify({'message': 'Employee deleted successfully'})
    except Exception as e:
        # Rollback the transaction if an error occurs
        session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        # Close the session
        session.close()

# Delete Department Endpoint
@app.route('/departments/<string:dept_no>', methods=['DELETE'])
@jwt_required()
def delete_department(dept_no):
    # Create a session
    session = Session()

    try:
        # Retrieve the department record to be deleted
        department = session.query(Department).filter_by(dept_no=dept_no).first()
        if not department:
            return jsonify({'error': 'Department not found'}), 404

        # Delete the department record
        session.delete(department)

        # Commit the changes to the database
        session.commit()

        return jsonify({'message': 'Department deleted successfully'})
    except Exception as e:
        # Rollback the transaction if an error occurs
        session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        # Close the session
        session.close()

# Delete Department Manager Endpoint
@app.route('/dept_managers/<int:emp_no>', methods=['DELETE'])
@jwt_required()
def delete_department_manager(emp_no):
    # Create a session
    session = Session()

    try:
        # Retrieve the department manager record to be deleted
        dept_manager = session.query(DeptManager).filter_by(emp_no=emp_no).first()
        if not dept_manager:
            return jsonify({'error': 'Department manager not found'}), 404

        # Delete the department manager record
        session.delete(dept_manager)

        # Commit the changes to the database
        session.commit()

        return jsonify({'message': 'Department manager deleted successfully'})
    except Exception as e:
        # Rollback the transaction if an error occurs
        session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        # Close the session
        session.close()

# Delete Department Assignment Endpoint
@app.route('/employees/<int:emp_no>/departments', methods=['DELETE'])
@jwt_required()
def delete_department_assignment(emp_no):
    # Create a session
    session = Session()

    try:
        # Retrieve the department assignment record to be deleted
        dept_assignment = session.query(DeptEmployee).filter_by(emp_no=emp_no).first()
        if not dept_assignment:
            return jsonify({'error': 'Department assignment not found'}), 404

        # Delete the department assignment record
        session.delete(dept_assignment)

        # Commit the changes to the database
        session.commit()

        return jsonify({'message': 'Department assignment deleted successfully'})
    except Exception as e:
        # Rollback the transaction if an error occurs
        session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        # Close the session
        session.close()

# Delete Employee Title Endpoint
@app.route('/employees/<int:emp_no>/title', methods=['DELETE'])
@jwt_required()
def delete_employee_title(emp_no):
    # Create a session
    session = Session()

    try:
        # Retrieve the title record to be deleted
        title_record = session.query(Title).filter_by(emp_no=emp_no).first()
        if not title_record:
            return jsonify({'error': 'Title record not found'}), 404

        # Delete the title record
        session.delete(title_record)

        # Commit the changes to the database
        session.commit()

        return jsonify({'message': 'Title record deleted successfully'})
    except Exception as e:
        # Rollback the transaction if an error occurs
        session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        # Close the session
        session.close()

# Delete Employee Salary Endpoint
@app.route('/employees/<int:emp_no>/salary', methods=['DELETE'])
@jwt_required()
def delete_employee_salary(emp_no):
    # Create a session
    session = Session()

    try:
        # Retrieve the salary record to be deleted
        salary_record = session.query(Salary).filter_by(emp_no=emp_no).first()
        if not salary_record:
            return jsonify({'error': 'Salary record not found'}), 404

        # Delete the salary record
        session.delete(salary_record)

        # Commit the changes to the database
        session.commit()

        return jsonify({'message': 'Salary record deleted successfully'})
    except Exception as e:
        # Rollback the transaction if an error occurs
        session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        # Close the session
        session.close()

# Delete Managerial Assignment Endpoint
@app.route('/employees/<int:emp_no>/managerial_assignment', methods=['DELETE'])
@jwt_required()
def delete_managerial_assignment(emp_no):
    # Create a session
    session = Session()

    try:
        # Retrieve the managerial assignment record to be deleted
        managerial_assignment_record = session.query(DeptManager).filter_by(emp_no=emp_no).first()
        if not managerial_assignment_record:
            return jsonify({'error': 'Managerial assignment record not found'}), 404

        # Delete the managerial assignment record
        session.delete(managerial_assignment_record)

        # Commit the changes to the database
        session.commit()

        return jsonify({'message': 'Managerial assignment record deleted successfully'})
    except Exception as e:
        # Rollback the transaction if an error occurs
        session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        # Close the session
        session.close()



if __name__ == '__main__':
    app.run(debug=True)