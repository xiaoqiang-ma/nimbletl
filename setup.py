import json

import setuptools


def parse_pipfile_lock():
    with open('Pipfile.lock') as f:
        lock_data = json.load(f)
    dependencies = lock_data['default']
    return [f"{pkg}{info.get('version', '')}" for pkg, info in dependencies.items()]


setuptools.setup(
    name='nimbletl',
    version='0.1.8',
    packages=setuptools.find_packages(),
    install_requires=parse_pipfile_lock(),
    entry_points={
        'console_scripts': [
            'nimbletl=log_driver.init.cli:main',
        ],
    },
    author="Ma Xiaoqiang",
    author_email="851788096@qq.com",
    description="Make your ETL easier and more nimble",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown'
)
