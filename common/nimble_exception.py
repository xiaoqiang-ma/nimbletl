class EtlFailedException(Exception):
    error_name = 'ETL is failed'

    def __init__(self, error_detail):
        self.error_detail = error_detail


class EtlIsRunningException(Exception):
    error_name = 'Etl is running'

    def __init__(self, error_detail):
        self.error_detail = error_detail
