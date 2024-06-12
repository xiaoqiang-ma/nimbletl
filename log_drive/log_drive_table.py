from sqlalchemy import Column, Integer, String, DateTime, Text

from common.nimbletl_db import Base


class LogDriveTable(Base):
    __tablename__ = 'log_drive_table'

    id = Column(Integer, primary_key=True, autoincrement=True)
    etl_name = Column(String(255), nullable=False)
    target_table_name = Column(String(255), nullable=False)
    drive_type = Column(String(255), nullable=False)
    drive_value = Column(String(255), nullable=False)
    process_start_time = Column(DateTime, nullable=False)
    process_end_time = Column(DateTime)
    etl_result = Column(Integer, nullable=False)
    business_time = Column(Text)

    def __repr__(self):
        return f"<LogDriveTable(id={self.id}, etl_name={self.etl_name}, target_table_name={self.target_table_name}, " \
               f"drive_type={self.drive_type}, drive_value={self.drive_value}, process_start_time={self.process_start_time}, " \
               f"process_end_time={self.process_end_time}, etl_result={self.etl_result}, business_time={self.business_time})>"
