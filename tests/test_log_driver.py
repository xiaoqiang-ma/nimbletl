from datetime import date, datetime
from typing import List

from log_driver.files import get_sorted_unexecuted_file_list
from log_driver.time_windows import get_sorted_unexecuted_for_time_window_1_param_day, \
    get_sorted_unexecuted_for_time_window_2_param
from log_driver.upstream import get_sorted_unexecuted_upstream_job_list

"""
INSERT INTO nimbletl.log_drive_table (id, etl_name, target_table_name, drive_type, drive_value, process_start_time, process_end_time, etl_result, business_time) VALUES (1, 'A_etl', 'a_target_name', 'time_window_1_param_day', '2024-04-01', '2024-05-29 17:34:00', '2024-05-29 17:34:04', 1, '2024-03-12, 2024-02-13');
INSERT INTO nimbletl.log_drive_table (id, etl_name, target_table_name, drive_type, drive_value, process_start_time, process_end_time, etl_result, business_time) VALUES (2, 'A_etl', 'a_target_name', 'time_window_1_param_day', '2024-04-03', '2024-05-29 17:34:03', '2024-05-29 17:34:06', 1, '2024-03-13, 2024-02-14');
INSERT INTO nimbletl.log_drive_table (id, etl_name, target_table_name, drive_type, drive_value, process_start_time, process_end_time, etl_result, business_time) VALUES (3, 'B_etl', 'b_target_name', 'time_window_2_param', '2024-05-01 00:00:00, 2024-05-03 00:00:00', '2024-05-29 18:43:32', '2024-05-29 18:43:34', 1, '2024-03-14, 2024-02-14');
INSERT INTO nimbletl.log_drive_table (id, etl_name, target_table_name, drive_type, drive_value, process_start_time, process_end_time, etl_result, business_time) VALUES (4, 'C_etl', 'c_target_name', 'files', 'a_20240404.xlsx', '2024-06-06 18:41:04', '2024-06-06 18:41:07', 1, '2024-03-15, 2024-02-15');
INSERT INTO nimbletl.log_drive_table (id, etl_name, target_table_name, drive_type, drive_value, process_start_time, process_end_time, etl_result, business_time) VALUES (5, 'D_etl', 'd_target_name', 'upstream', 'A_etl|1', '2024-06-07 11:35:45', '2024-06-07 11:35:47', 1, '2024-03-16, 2024-02-16');
INSERT INTO nimbletl.log_drive_table (id, etl_name, target_table_name, drive_type, drive_value, process_start_time, process_end_time, etl_result, business_time) VALUES (6, 'A_etl', 'a_target_name', 'time_window_1_param_day', '2024-04-02', '2024-05-29 17:34:03', '2024-05-29 17:34:06', 1, '2024-03-17, 2024-02-17');

"""


def test_get_sorted_unexecuted_for_time_window_1_param_day():
    etl_name = 'A_etl'
    start_date = date(2024, 3, 26)
    result = get_sorted_unexecuted_for_time_window_1_param_day(etl_name, start_date)
    print(result)


def test_get_sorted_unexecuted_for_time_window_2_param():
    etl_name = 'B_etl'
    start_datetime = datetime.strptime('2024-05-01 00:00:00', "%Y-%m-%d %H:%M:%S")
    time_interval = 2880
    result = get_sorted_unexecuted_for_time_window_2_param(etl_name, start_datetime, time_interval)
    print(result)


def test_get_sorted_unexecuted_file_list():
    def get_file_list() -> List[str]:
        return ['a_20240403.xlsx', 'a_20240404.xlsx', 'a_20240405.xlsx']

    etl_name = 'C_etl'
    result = get_sorted_unexecuted_file_list(get_file_list, etl_name)
    print(result)


def test_get_sorted_unexecuted_upstream_job_list():
    etl_name = 'D_etl'
    upstream_etl_name = 'A_etl'
    result = get_sorted_unexecuted_upstream_job_list(etl_name, upstream_etl_name)
    print(result)


if __name__ == '__main__':
    test_get_sorted_unexecuted_for_time_window_1_param_day()
    test_get_sorted_unexecuted_for_time_window_2_param()
    test_get_sorted_unexecuted_file_list()
    test_get_sorted_unexecuted_upstream_job_list()
