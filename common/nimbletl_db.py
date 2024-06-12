import os

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


def _get_drive_db_url():
    db_user = os.getenv('LOG_DRIVE_DB_USER', 'root')
    db_password = os.getenv('LOG_DRIVE_DB_PASSWORD', '')
    db_host = os.getenv('LOG_DRIVE_DB_HOST', 'localhost')
    db_port = os.getenv('LOG_DRIVE_DB_PORT', '3306')
    db_name = os.getenv('LOG_DRIVE_DB_NAME', 'nimbletl')
    return f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'


DATABASE_URI = _get_drive_db_url()
drive_db_engine = create_engine(DATABASE_URI)
session = sessionmaker(bind=drive_db_engine)()


def get_dataframe(sql_str):
    try:
        # 使用同一个数据库连接对象
        engine = drive_db_engine
        df = pd.read_sql_query(sql_str, con=engine.connect())
        return df
    except SQLAlchemyError as e:
        print(f"Error occurred: {e}")
    finally:
        if engine:
            engine.dispose()


def execute_sql(sql_str):
    try:
        # 使用同一个数据库连接对象
        engine = drive_db_engine
        with engine.connect() as connection:
            connection.execute(sql_str)
        print("Table created successfully.")
    except SQLAlchemyError as e:
        print(f"Error occurred: {e}")
    finally:
        if connection:
            connection.close()


def save_obj(ojb: Base):
    try:
        session.add(ojb)
        session.commit()
        return ojb.id
    except Exception as e:
        session.rollback()
        print(f"Error inserting log: {e}")
    finally:
        session.close()


