from sqlalchemy import create_engine, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

"""Postgres simple connection module
"""

engine = create_engine("postgresql://airflow:airflow@172.18.0.3:5432/airflow")
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()
