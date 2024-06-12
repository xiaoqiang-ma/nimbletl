from common.nimbletl_db import execute_sql
from log_drive.init import LOG_DRIVE_DDL


def initialize_database():
    execute_sql(LOG_DRIVE_DDL)
