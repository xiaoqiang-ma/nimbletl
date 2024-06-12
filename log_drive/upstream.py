from datetime import datetime, date
from typing import Tuple, List

from common.nimbletl_db import get_dataframe


def _get_successful_log_for_upstream(etl_name: str) -> List[int]:
    sql_str = f"""
        SELECT drive_value
        FROM log_drive_table
        WHERE etl_name = '{etl_name}' and etl_result = 1
        order by drive_value;
    """

    result_df = get_dataframe(sql_str)

    def get_upstream_id(x):
        return int(x.split('|')[1].strip())

    result_list = result_df['drive_value'].apply(get_upstream_id).tolist()

    return result_list


def _get_all_successful_job_list_for_upstream(upstream_etl_name: str) -> List[int]:
    sql_str = f"""
            SELECT id
            FROM log_drive_table
            WHERE etl_name = '{upstream_etl_name}' and etl_result = 1
            order by drive_value;
        """

    result_df = get_dataframe(sql_str)
    result_list = result_df['id'].apply(lambda x: int(x)).tolist()

    return result_list


def _get_unprocessed_file_list_by_upstream_id(unprocessed_file_list) -> List[Tuple[int, List[date]]]:
    ids_str = ', '.join(map(str, unprocessed_file_list))
    sql_str = f"SELECT * FROM log_drive_table WHERE id IN ({ids_str})"
    result_df = get_dataframe(sql_str)

    def get_business_time(x):
        business_time_list = x['business_time'].split(',')
        business_date_list = [datetime.strptime(date_str.strip(), "%Y-%m-%d").date() for date_str in business_time_list]
        return tuple([x['id'], business_date_list])

    result_list = result_df.apply(get_business_time, axis=1).tolist()
    return result_list


def get_sorted_unexecuted_upstream_job_list(etl_name: str, upstream_etl_name: str) -> List[Tuple[int, List[date]]]:
    executed_successful_file_list = _get_successful_log_for_upstream(etl_name)

    all_successful_job_list = _get_all_successful_job_list_for_upstream(upstream_etl_name)

    unprocessed_file_list = list(set(all_successful_job_list).symmetric_difference(set(executed_successful_file_list)))
    unprocessed_file_list.sort()

    result_list = _get_unprocessed_file_list_by_upstream_id(unprocessed_file_list)

    return result_list
