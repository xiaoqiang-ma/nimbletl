import datetime
from typing import List

from common.db import get_dataframe
from common.time_util import get_now_date


def _get_successful_log_for_time_window_1_param_day(etl_name, start_date_str) -> List[str]:
    """
    获取log表中已执行成功的日期list
    :param etl_name:
    :param start_date_str:
    :return:
    """

    sql_str = f"""
        SELECT drive_value
        FROM log_drive_table
        WHERE etl_name = '{etl_name}' and etl_result = 1 and drive_value >= '{start_date_str}'
        order by drive_value;
    """

    result_df = get_dataframe(sql_str)
    return list(result_df['drive_value'])


def _get_all_date_list_for_time_window_1_param_day(start_date_str, date_offset=1) -> List[str]:
    """
    :param start_date_str: 本次数据开始时间，格式：yyyy-MM-dd
    :param date_offset: 返回的date list的最大date和当前日期的差值，默认差一天.如，今天是04月04日，则返回的date list最大值为04月03.
    :return:
    """
    # 将开始日期字符串转换为 date 对象
    start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').date()

    # 获取当前日期
    now_date = get_now_date()

    # 获取最后日期
    end_date = now_date + datetime.timedelta(days=-date_offset)

    # 获取开始时间到当前时间的天数
    interval_days = (now_date - start_date).days

    # 生成日期列表
    date_list = []
    for i in range(interval_days):
        # 计算偏移后的日期
        current_date = start_date + datetime.timedelta(days=i)

        # 如果偏移后的日期超过了当前日期，则停止生成
        if current_date > end_date:
            break

        # 将日期转换为字符串，并添加到列表中
        date_list.append(current_date.strftime('%Y-%m-%d'))

    return date_list


def get_sorted_unexecuted_for_time_window_1_param_day(etl_name=None, start_date_str=None, date_offset=1) -> List[str]:
    """
    日志驱动类型：time_window_1_param_day
    :param etl_name: ETL 名称
    :param start_date_str: ETL对应数据的初始化时间，格式：yyyy-MM-dd
    :param date_offset: 日期偏移量，返回的date list的最大date和当前日期的差值，默认差一天.如，今天是04月04日，则返回的date list最大值为04月03.
    :return: 返回待处理日期的list
    """
    if etl_name is None:
        raise ValueError("No parameter etl_name was passed.")
    if start_date_str is None:
        raise ValueError("No parameter start_date_str was passed.")

    executed_successful_date_list = _get_successful_log_for_time_window_1_param_day(etl_name, start_date_str)

    all_date_list = _get_all_date_list_for_time_window_1_param_day(start_date_str, date_offset)

    unprocessed_date_list = [date_temp for date_temp in all_date_list if
                             date_temp not in executed_successful_date_list]
    unprocessed_date_list.sort()
    return unprocessed_date_list
