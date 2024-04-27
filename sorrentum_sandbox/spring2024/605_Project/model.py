from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import reconstructor
from app import db
db = SQLAlchemy()
class Catalog(db.Model):
    __tablename__ = 'catalog'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(100), nullable=False)
    description = db.Column(db.Text)
    #products = db.relationship('Product', backref='catalog', lazy=True)

class Manufacturer(db.Model):
    __tablename__ = 'manufacturer'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(100), nullable=False)
    email = db.Column(db.String(255))
    phone = db.Column(db.String(20))
    address = db.Column(db.String(255))
    status = db.Column(db.String(50))
    #products = db.relationship('Product', backref='manufacturer', lazy=True)

class Doctor(db.Model):
    __tablename__ = 'doctor'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(100), nullable=False)
    specialization = db.Column(db.String(50))
    age = db.Column(db.Integer)
    phone = db.Column(db.String(20))
    email = db.Column(db.String(255))
    address = db.Column(db.String(255))
    status = db.Column(db.String(50))
    #orders = db.relationship('Orders', backref='doctor', lazy=True)

class Customer(db.Model):
    __tablename__ = 'customer'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(100), nullable=False)
    age = db.Column(db.Integer)
    phone = db.Column(db.String(20))
    email = db.Column(db.String(255))
    address = db.Column(db.String(255))
    gender = db.Column(db.String(20))
    #orders = db.relationship('Orders', backref='customer', lazy=True)

class Product(db.Model):
    __tablename__ = 'product'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(100), nullable=False)
    product_use = db.Column(db.Text)
    count = db.Column(db.Integer, nullable=False)
    price = db.Column(db.Numeric, nullable=False)
    expiry = db.Column(db.Date)
    catalog_name = db.Column(db.String(100), nullable=False)
    manufacturer_name = db.Column(db.String(100), nullable=False)
    catalog_id = db.Column(db.Integer, db.ForeignKey('catalog.id'), nullable=False)
    manufacturer_id = db.Column(db.Integer, db.ForeignKey('manufacturer.id'), nullable=False)


class Pharmacist(db.Model):
    __tablename__ = 'pharmacist'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(100), nullable=False)
    password = db.Column(db.String(100), nullable=False)
    status = db.Column(db.String(50))
    #orders = db.relationship('Orders', backref='pharmacist', lazy=True)

class Orders(db.Model):
    __tablename__ = 'orders'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    time = db.Column(db.DateTime)
    product_ids =  db.Column(db.String(1000), nullable=False)
    product_names = db.Column(db.String(1000), nullable=False)
    product_counts =  db.Column(db.String(1000), nullable=False)
    total = db.Column(db.Numeric, nullable=False)
    payment_method = db.Column(db.String(20))
    status = db.Column(db.String(20))
    customer_name = db.Column(db.String(100), nullable=False)
    doctor_name = db.Column(db.String(100), nullable=False)

    pharmacist_id = db.Column(db.Integer, db.ForeignKey('pharmacist.id'), unique=True)
    customer_id = db.Column(db.Integer, db.ForeignKey('customer.id'), unique=True)
    doctor_id = db.Column(db.Integer, db.ForeignKey('doctor.id'), unique=True)
    #products = db.relationship('Product', backref='order', lazy=True)

    @reconstructor
    def init_on_load(self):
        self.product_ids = self._get_product_ids()
        self.product_names = self._get_product_names()
        self.product_counts = self._get_product_counts()

    def _get_product_ids(self):
        return ','.join(str(product.id) for product in self.products)

    def _get_product_names(self):
        return ','.join(product.name for product in self.products)

    def _get_product_counts(self):
        return ','.join(str(product.count) for product in self.products)

    def set_product_counts(self, counts):
        #validate and set the product counts
        for count in counts:
            if count < 0:
                raise ValueError("Product count must be non-negative.")
        self.product_counts = ','.join(str(count) for count in counts)

    def validate_product_counts(self):
        #validate that product counts are less than or equal to product counts for each associated product
        order_product_counts = [int(count) for count in self.product_counts.split(',')]
        for product, count in zip(self.products, order_product_counts):
            if count > product.count:
                raise ValueError(f"Order product count for {product.name} exceeds available stock.")

    def before_insert(self):
        #validate product counts before inserting
        self.validate_product_counts()

    def before_update(self):
        #validate product counts before updating
        self.validate_product_counts()


class Shipment(db.Model):
    __tablename__ = 'shipment'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    dispatch_date = db.Column(db.DateTime)
    delivery_date = db.Column(db.Date)
    status = db.Column(db.String(20))
    order_id = db.Column(db.Integer, db.ForeignKey('orders.id'), nullable=False)
    order_time = db.Column(db.DateTime)