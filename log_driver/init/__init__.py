LOG_DRIVE_DDL = """
    CREATE TABLE log_drive_table (
        id INT NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT '日志唯一标识',
        etl_name VARCHAR(255) NOT NULL COMMENT 'ETL 任务名称',
        target_table_name VARCHAR(255) NOT NULL COMMENT 'ETL 目标表名称',
        drive_type VARCHAR(255) NOT NULL COMMENT '日志驱动类型，目前枚举值包括：time_window_1_param_day,time_window_1_param_week,time_window_2_param,upstream',
        drive_value VARCHAR(255) NOT NULL COMMENT '日志驱动值',
        process_start_time DATETIME NOT NULL COMMENT 'ETL 处理开始时间',
        process_end_time DATETIME NOT NULL COMMENT 'ETL 处理结束时间',
        etl_result INT NOT NULL COMMENT 'ETL 处理结果,目前枚举值包括：0-执行失败；1-执行成功；2-执行中',
        business_time VARCHAR(255) COMMENT '本批次数据涉及到的业务日期，逗号分割'
    ) COMMENT='日志驱动表';
"""