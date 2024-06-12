import os
import platform

import yaml


def load_config(file_path):
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"Configuration file {file_path} does not exist.")

    with open(file_path, 'r') as stream:
        try:
            config = yaml.safe_load(stream)
            for key, value in config.items():
                os.environ[key] = str(value)
                # 根据操作系统类型设置永久环境变量
                set_permanent_env_var(key, str(value))
        except yaml.YAMLError as exc:
            raise ValueError(f"Error parsing YAML file: {exc}")


def set_permanent_env_var(key, value):
    system = platform.system()
    shell_profile_path = get_shell_profile_path(system)
    with open(shell_profile_path, 'r') as f:
        lines = f.readlines()

    # 检查是否已经存在相同的配置
    existing_line = None
    for i, line in enumerate(lines):
        if f'export {key}=' in line:
            existing_line = i
            break

    if existing_line is not None:
        # 如果已经存在相同的配置，则覆盖原有的配置
        lines[existing_line] = f'export {key}={value}\n'
    else:
        # 否则，添加新的配置
        lines.append(f'export {key}={value}\n')

    # 将更新后的配置写回到文件中
    with open(shell_profile_path, 'w') as f:
        f.writelines(lines)


def get_shell_profile_path(system):
    # 获取当前 shell 的配置文件路径
    if system == "Windows":
        return None  # Windows 暂时不支持永久环境变量
    elif system == "Darwin":
        return get_mac_shell_profile_path()
    elif system == "Linux":
        return get_linux_shell_profile_path()
    else:
        raise ValueError("Unsupported operating system.")


def get_mac_shell_profile_path():
    # 获取 Linux 操作系统下的 shell 配置文件路径
    shell = os.environ.get('SHELL', '')
    if "zsh" in shell:
        return os.path.expanduser('~/.zshrc')
    elif "bash" in shell:
        return os.path.expanduser('~/.bash_profile')
    else:
        raise ValueError("Unsupported shell.")


def get_linux_shell_profile_path():
    # 获取 Linux 操作系统下的 shell 配置文件路径
    shell = os.environ.get('SHELL', '')
    if "zsh" in shell:
        return os.path.expanduser('~/.zshrc')
    elif "bash" in shell:
        return os.path.expanduser('~/.bashrc')
    else:
        raise ValueError("Unsupported shell.")
