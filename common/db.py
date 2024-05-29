import os

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


def _get_drive_db_url():
    db_user = os.getenv('LOG_DRIVE_DB_USER', 'root')
    db_password = os.getenv('LOG_DRIVE_DB_PASSWORD', '')
    db_host = os.getenv('LOG_DRIVE_DB_HOST', 'localhost')
    db_port = os.getenv('LOG_DRIVE_DB_PORT', '3306')
    db_name = os.getenv('LOG_DRIVE_DB_NAME', 'nimbletl')
    return f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'


def _get_engine():
    return create_engine(_get_drive_db_url())


def get_dataframe(sql_str):
    try:
        # 使用同一个数据库连接对象
        engine = _get_engine()
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
        engine = _get_engine()
        with engine.connect() as connection:
            connection.execute(sql_str)
        print("Table created successfully.")
    except SQLAlchemyError as e:
        print(f"Error occurred: {e}")
    finally:
        if connection:
            connection.close()
