from datetime import datetime

from common.nimble_enum import EtlStatusEnum
from common.nimbletl_db import get_dataframe, session
from common.nimble_exception import EtlIsRunningException
from log_drive.log_drive_table import LogDriveTable


def check_etl_is_running_and_warning(etl_name: str):
    if _check_etl_is_running(etl_name):
        raise EtlIsRunningException(f'[etl_name] ETL正在执行中...')


def _check_etl_is_running(etl_name: str) -> bool:
    """
    判断当前对应的ETL是否有在执行中
    :return:
    """
    last_etl_job_result_str = f'''
        SELECT top 1 etl_result
        FROM log_drive_table
        WHERE etl_name = '{etl_name}'
        order by id desc;
    '''
    result_df = get_dataframe(last_etl_job_result_str)
    if result_df.empty:
        return False
    return result_df['etl_result'][0] == 2


def set_log_success(log_id: int):
    drive_log_entry = session.query(LogDriveTable).filter_by(id=log_id).first()
    if drive_log_entry:
        drive_log_entry.etl_result = EtlStatusEnum.SUCCEED.value
        drive_log_entry.process_end_time = datetime.now()
        session.commit()
        print(f"set log to succeed with id: {log_id}")


def set_log_failed():
    pass



