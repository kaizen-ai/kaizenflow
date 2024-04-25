from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from models import Base
import os

# Specify the path to the SQLite database file
db_folder = '/Users/josephsketl/Docs/School/Data605/Project/employee_db'
db_file = os.path.join(db_folder, 'employees.db')

# Create a SQLAlchemy engine
engine = create_engine(f'sqlite:///{db_file}')
Base.metadata.create_all(engine)

# Create a session maker
Session = sessionmaker(bind=engine)

# Data loading logic
def load_dump_file(session, file_path):
    with open(file_path, 'r') as file:
        # Read the entire content of the file
        file_content = file.read()
        
        # Split the content into individual SQL statements
        sql_commands = file_content.split(';')
        
        # Execute each SQL command
        for sql_command in sql_commands:
            sql_command = sql_command.strip()
            if sql_command:  # Skip empty commands
                session.execute(text(sql_command))

# Create a session
with Session() as session:
    try:
        # Load data from each dump file into the database
        load_dump_file(session, '/Users/josephsketl/Docs/School/Data605/Project/employee_db/load_departments.dump')
        load_dump_file(session, '/Users/josephsketl/Docs/School/Data605/Project/employee_db/load_employees.dump')
        load_dump_file(session, '/Users/josephsketl/Docs/School/Data605/Project/employee_db/load_dept_emp.dump')
        load_dump_file(session, '/Users/josephsketl/Docs/School/Data605/Project/employee_db/load_dept_manager.dump')
        load_dump_file(session, '/Users/josephsketl/Docs/School/Data605/Project/employee_db/load_titles.dump')
        load_dump_file(session, '/Users/josephsketl/Docs/School/Data605/Project/employee_db/load_salaries1.dump')
        load_dump_file(session, '/Users/josephsketl/Docs/School/Data605/Project/employee_db/load_salaries2.dump')
        load_dump_file(session, '/Users/josephsketl/Docs/School/Data605/Project/employee_db/load_salaries3.dump')

        # Commit the changes to persist data in the database
        session.commit()
        print("Data loaded successfully!")
    except Exception as e:
        print(f"An error occurred: {e}")
        # Rollback the session in case of error
        session.rollback()
