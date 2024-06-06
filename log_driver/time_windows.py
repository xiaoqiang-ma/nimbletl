from datetime import datetime, timedelta, date
from typing import List, Tuple

from common.db import get_dataframe
from common.time_util import get_now_date


def _get_successful_log_for_time_window_1_param_day(etl_name: str) -> List[date]:
    """
    获取log表中已执行成功的日期list
    :param etl_name:
    :return:
    """

    sql_str = f"""
        SELECT drive_value
        FROM log_drive_table
        WHERE etl_name = '{etl_name}' and etl_result = 1
        order by drive_value;
    """

    result_df = get_dataframe(sql_str)

    def str_to_date(date_str):
        return datetime.strptime(date_str, "%Y-%m-%d").date()

    result_list = result_df['drive_value'].apply(str_to_date).tolist()

    return result_list


def _get_successful_log_for_time_window_2_param(etl_name: str) -> List[Tuple[datetime, datetime]]:
    """
    获取log表中已执行成功的时间序列list
    :param etl_name:
    :return:
    """

    sql_str = f"""
        SELECT drive_value
        FROM log_drive_table
        WHERE etl_name = '{etl_name}' and etl_result = 1
        order by drive_value;
    """

    result_df = get_dataframe(sql_str)

    def str_to_tuple(date_str):
        return tuple([datetime.strptime(tmp.strip(), '%Y-%m-%d %H:%M:%S') for tmp in date_str.split(',')])

    result_list = result_df['drive_value'].apply(str_to_tuple).tolist()
    return result_list


def _get_all_date_list_for_time_window_1_param_day(start_date: date, date_offset: int) -> List[date]:
    """

    :param start_date: 本次数据开始时间
    :param date_offset: 返回的date list的最大date和当前日期的差值，默认差一天.如，今天是04月04日，则返回的date list最大值为04月03.
    :return:
    """

    # 获取当前日期
    now_date = get_now_date()

    # 获取最后日期
    end_date = now_date + timedelta(days=-date_offset)

    # 获取开始时间到当前时间的天数
    interval_days = (now_date - start_date).days

    # 生成日期列表
    date_list = []
    for i in range(interval_days):
        # 计算偏移后的日期
        current_date = start_date + timedelta(days=i)

        # 如果偏移后的日期超过了当前日期，则停止生成
        if current_date > end_date:
            break

        date_list.append(current_date)

    return date_list


def get_sorted_unexecuted_for_time_window_1_param_day(etl_name: str = None, start_date: date = None,
                                                      date_offset: int = 1) -> List[date]:
    """
    日志驱动类型：time_window_1_param_day
    :param etl_name: ETL 名称
    :param start_date: ETL对应数据的初始化时间
    :param date_offset: 日期偏移量，返回的date list的最大date和当前日期的差值，默认差一天.如，今天是04月04日，则返回的date list最大值为04月03.
    :return: 返回待处理日期的list
    """
    if etl_name is None:
        raise ValueError("No parameter etl_name was passed.")
    if start_date is None:
        raise ValueError("No parameter start_date_str was passed.")

    executed_successful_date_list = _get_successful_log_for_time_window_1_param_day(etl_name)

    all_date_list = _get_all_date_list_for_time_window_1_param_day(start_date, date_offset)

    unprocessed_date_list = [date_temp for date_temp in all_date_list if
                             date_temp not in executed_successful_date_list]
    unprocessed_date_list.sort()
    return unprocessed_date_list


def _get_all_date_list_for_time_window_2_param(start_datetime: datetime, time_interval: int) -> List[Tuple[datetime, datetime]]:
    """
    :param start_datetime: 开始时间
    :param time_interval: 时间间隔，单位为分钟。
    :return: 包含时间间隔的元组列表，每个元组包含开始时间和结束时间。
    """

    # 获取当前时间
    end_datetime = datetime.now()

    # 初始化结果列表
    time_intervals = []

    # 开始时间
    current_datetime = start_datetime

    # 生成时间间隔列表
    while current_datetime < end_datetime:
        # 计算结束时间
        end_interval = current_datetime + timedelta(minutes=time_interval)

        # 如果结束时间超过当前时间，则设置结束时间为当前时间
        if end_interval > end_datetime:
            break

        # 添加时间间隔到结果列表中
        time_intervals.append((current_datetime, end_interval))

        # 更新当前时间为结束时间，以便下一次循环
        current_datetime = end_interval

    return time_intervals


def get_sorted_unexecuted_for_time_window_2_param(etl_name: str = None, start_datetime: datetime = None,
                                                  time_interval: int = 1440) -> List[Tuple[datetime, datetime]]:
    """
    从给定的开始时间开始，以给定的时间间隔生成时间序列。返回未执行的时间序列
    :param etl_name:
    :param start_datetime:
    :param time_interval: 单位分钟。默认1440秒
    :return:
    """
    if etl_name is None:
        raise ValueError("No parameter etl_name was passed.")
    if start_datetime is None:
        raise ValueError("No parameter start_date_str was passed.")

    executed_successful_date_list = _get_successful_log_for_time_window_2_param(etl_name)

    all_date_list = _get_all_date_list_for_time_window_2_param(start_datetime, time_interval)

    unprocessed_date_list = list(set(all_date_list).symmetric_difference(set(executed_successful_date_list)))
    unprocessed_date_list.sort()
    return unprocessed_date_list
