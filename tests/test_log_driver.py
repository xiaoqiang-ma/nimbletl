from datetime import date, datetime
from typing import List

from log_driver.files import get_sorted_unexecuted_file_list
from log_driver.time_windows import get_sorted_unexecuted_for_time_window_1_param_day, \
    get_sorted_unexecuted_for_time_window_2_param


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


if __name__ == '__main__':
    test_get_sorted_unexecuted_for_time_window_1_param_day()
    test_get_sorted_unexecuted_for_time_window_2_param()
    test_get_sorted_unexecuted_file_list()
