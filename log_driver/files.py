from typing import Callable, List

from common.db import get_dataframe


def _get_successful_log_for_files(etl_name: str) -> List[str]:
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

    result_list = result_df['drive_value'].tolist()

    return result_list


def get_sorted_unexecuted_file_list(get_file_list_from_file_systems: Callable[..., List[str]], etl_name: str, *args,
                                    **kwargs) -> List[str]:
    """
    获取待执行的文件名称列表
    :param get_file_list_from_file_systems:
    :param etl_name:
    :param args:
    :param kwargs:
    :return:
    """
    executed_successful_file_list = _get_successful_log_for_files(etl_name)
    all_file_list = get_file_list_from_file_systems(*args, **kwargs)
    unprocessed_file_list = list(set(all_file_list).symmetric_difference(set(executed_successful_file_list)))
    unprocessed_file_list.sort()
    return unprocessed_file_list
