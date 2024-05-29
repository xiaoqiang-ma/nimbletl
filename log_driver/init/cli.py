import click

from log_driver.init.init_drive_db import initialize_database
from log_driver.init.load_config_to_env import load_config


@click.group()
def main():
    pass


@main.command()
@click.option('--file', type=click.Path(exists=True), required=True, help="Path to the YAML configuration file.")
def load_config_to_env(file):
    """Load configuration file and set environment variables."""
    try:
        load_config(file)
        click.echo("Configuration loaded log drive db config successfully.")
    except Exception as e:
        click.echo(f"Error: {e}")


@main.command()
def init_drive_db():
    """Initialize the database with the configuration from environment variables."""
    try:
        initialize_database()
        click.echo("Log drive database initialized successfully.")
    except Exception as e:
        click.echo(f"Error: {e}")


if __name__ == '__main__':
    main()
