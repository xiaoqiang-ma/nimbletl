import datetime
from typing import List, Tuple

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


def _get_successful_log_for_time_window_2_param(etl_name) -> List[Tuple[str, str]]:
    """
    获取log表中已执行成功的时间序列list
    :param etl_name:
    :param start_date_str:
    :return:
    """

    sql_str = f"""
        SELECT drive_value
        FROM log_drive_table
        WHERE etl_name = '{etl_name}' and etl_result = 1
        order by drive_value;
    """

    result_df = get_dataframe(sql_str)

    result_list = []
    for drive_value in result_df['drive_value']:
        result_list.append(tuple(tmp.strip() for tmp in drive_value.split(',')))
    return result_list


def _get_all_date_list_for_time_window_2_param(start_datetime_str, time_interval) -> List[Tuple[str, str]]:
    """
    从给定的开始时间开始，以给定的时间间隔生成时间序列。

    参数：
    start_datetime_str : str
        开始时间字符串，格式为 'yyyy-MM-dd HH:mm:ss'。
    time_interval : int
        时间间隔，单位为分钟。

    返回：
    list
        包含时间间隔的元组列表，每个元组包含开始时间和结束时间。
    """
    # 将开始时间字符串转换为 datetime 对象
    start_datetime = datetime.datetime.strptime(start_datetime_str, '%Y-%m-%d %H:%M:%S')

    # 获取当前时间
    end_datetime = datetime.datetime.now()

    # 初始化结果列表
    time_intervals = []

    # 开始时间
    current_datetime = start_datetime

    # 生成时间间隔列表
    while current_datetime < end_datetime:
        # 计算结束时间
        end_interval = current_datetime + datetime.timedelta(minutes=time_interval)

        # 如果结束时间超过当前时间，则设置结束时间为当前时间
        if end_interval > end_datetime:
            break

        # 添加时间间隔到结果列表中
        time_intervals.append((current_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                               end_interval.strftime('%Y-%m-%d %H:%M:%S')))

        # 更新当前时间为结束时间，以便下一次循环
        current_datetime = end_interval

    return time_intervals


def get_sorted_unexecuted_for_time_window_2_param_day(etl_name=None, start_datetime_str=None, time_interval=1440) -> List[Tuple[str, str]]:
    """
    从给定的开始时间开始，以给定的时间间隔生成时间序列。返回未执行的时间序列
    :param etl_name:
    :param start_datetime_str: 格式：'%Y-%m-%d %H:%M:%S
    :param time_interval: 单位分钟。默认1440秒
    :return:
    """
    if etl_name is None:
        raise ValueError("No parameter etl_name was passed.")
    if start_datetime_str is None:
        raise ValueError("No parameter start_date_str was passed.")

    executed_successful_date_list = _get_successful_log_for_time_window_2_param(etl_name)

    all_date_list = _get_all_date_list_for_time_window_2_param(start_datetime_str, time_interval)

    unprocessed_date_list = [date_temp for date_temp in all_date_list if
                             date_temp not in executed_successful_date_list]
    unprocessed_date_list.sort()
    return unprocessed_date_list
