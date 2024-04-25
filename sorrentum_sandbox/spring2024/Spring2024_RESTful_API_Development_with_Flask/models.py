# models.py
from sqlalchemy import Column, Integer, String, Date, Enum, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Employee(Base):
    __tablename__ = 'employees'

    emp_no = Column(Integer, primary_key=True)
    birth_date = Column(Date, nullable=False)
    first_name = Column(String(14), nullable=False)
    last_name = Column(String(16), nullable=False)
    gender = Column(Enum('M', 'F'), nullable=False)
    hire_date = Column(Date, nullable=False)

class Department(Base):
    __tablename__ = 'departments'

    dept_no = Column(String(4), primary_key=True)
    dept_name = Column(String(40), nullable=False)

class DeptManager(Base):
    __tablename__ = 'dept_manager'

    emp_no = Column(Integer, ForeignKey('employees.emp_no'), primary_key=True)
    dept_no = Column(String(4), ForeignKey('departments.dept_no'), primary_key=True)
    from_date = Column(Date, nullable=False)
    to_date = Column(Date, nullable=False)
    employee = relationship('Employee')
    department = relationship('Department')

class DeptEmployee(Base):
    __tablename__ = 'dept_emp'

    emp_no = Column(Integer, ForeignKey('employees.emp_no'), primary_key=True)
    dept_no = Column(String(4), ForeignKey('departments.dept_no'), primary_key=True)
    from_date = Column(Date, nullable=False)
    to_date = Column(Date, nullable=False)
    employee = relationship('Employee')
    department = relationship('Department')

class Title(Base):
    __tablename__ = 'titles'
    emp_no = Column(Integer, ForeignKey('employees.emp_no'), primary_key=True)
    title = Column(String(50), nullable=False)
    from_date = Column(Date, primary_key=True)  
    to_date = Column(Date, nullable=False)

class Salary(Base):
    __tablename__ = 'salaries'
    emp_no = Column(Integer, ForeignKey('employees.emp_no'), primary_key=True)
    salary = Column(Integer, nullable=False)
    from_date = Column(Date, primary_key=True)
    to_date = Column(Date, nullable=False)
# Define other models similarly for tables: dept_emp, titles, salaries
