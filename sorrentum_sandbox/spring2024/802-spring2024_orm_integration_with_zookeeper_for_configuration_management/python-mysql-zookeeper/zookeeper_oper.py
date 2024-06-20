from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from kazoo.client import KazooClient
import os

# init ZooKeeper clien
zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()

# from ZooKeeper get database config
db_config, _ = zk.get("/configs/database_url")
db_uri = db_config.decode('utf-8') if db_config else 'sqlite:///example.db'
print(db_uri+"***************************")
# init SQLAlchemy
Base = declarative_base()
engine = create_engine(db_uri)
Session = sessionmaker(bind=engine)

class ApplicationConfig(Base):
    __tablename__ = 'application_config'
    id = Column(Integer, primary_key=True)
    key = Column(String(50), unique=True)
    value = Column(String(50))

    def __repr__(self):
        return f"<ApplicationConfig(key='{self.key}', value='{self.value}')>"

# creat table
Base.metadata.create_all(engine)

# file session
session = Session()