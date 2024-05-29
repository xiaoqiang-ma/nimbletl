import click
import mysql.connector
from mysql.connector import errorcode


def initialize_database(config):
    click.echo("""Log table creation...""")
    db_config = config.get('database', {})
    try:
        connection = mysql.connector.connect(
            host=db_config.get('host', 'localhost'),
            user=db_config.get('user', 'root'),
            password=db_config.get('password', ''),
            database=db_config.get('database', '')
        )
        cursor = connection.cursor()

        # Example: Create a sample table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS example_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        connection.commit()
        cursor.close()
        connection.close()
        print("Database initialized successfully.")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Error: Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Error: Database does not exist")
        else:
            print(err)
    else:
        connection.close()
        click.echo("""The log table is created successfully.""")
