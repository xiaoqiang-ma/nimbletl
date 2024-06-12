from enum import Enum


class EtlStatusEnum(Enum):
    SUCCEED = 1
    FAILED = 0
    RUNNING = 2


class LogDriveType(Enum):
    TIME_WINDOW_1_PARAM_DAY = "time_window_1_param_day"
    TIME_WINDOW_2_PARAM = "time_window_2_param"
    UPSTREAM = "UPSTREAM"
    FILES = "files"
