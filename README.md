# NimbleTL

NimbleTL is a Python package to facilitate ETL development with a focus on quick and easy initialization of MySQL database tables.

## Installation

To install NimbleTL, run:

```bash
pip install nimbletl

将配置文件写入环境变量中
nimbletl load-config-to-env --file ./log_drive_db_config.yml

刷新环境变量，需要知道写入到那个环境变量文件中
source ~/.zshrc
source ~/.bashrc
source ~/.bash_profile

nimbletl init-drive-db
