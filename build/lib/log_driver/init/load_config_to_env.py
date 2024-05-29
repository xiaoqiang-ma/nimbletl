import os
import yaml


def load_config(file_path):
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"Configuration file {file_path} does not exist.")

    with open(file_path, 'r') as stream:
        try:
            config = yaml.safe_load(stream)
            for key, value in config.items():
                os.environ[key] = str(value)
        except yaml.YAMLError as exc:
            raise ValueError(f"Error parsing YAML file: {exc}")
