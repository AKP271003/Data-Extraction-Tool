import os
import mysql.connector  # pip install mysql-connector-python

def generate_config():
    # Database connection details (fill as needed)
    db_config = {
        'host': 'localhost',
        'user': 'your_username',
        'password': 'your_password',
        'database': 'your_database'
    }

    config_path = 'config.txt'

    queries = [
        ('query1', 'SELECT {select_clause} FROM {table_name} WHERE {column_name} IN (%s)'),
        ('query2', 'SELECT {select_clause} FROM {table_name} WHERE {column_name} LIKE "%s%"'),
    ]

    try:
        # Connect to the database
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Fetch table and column names
        fetch_query = """
            SELECT table_name, column_name 
            FROM information_schema.columns 
            WHERE table_schema = %s
        """
        cursor.execute(fetch_query, (db_config['database'],))
        tables_columns = cursor.fetchall()

        # Write to config file
        with open(config_path, 'w') as file:
            file.write("# Generated config.txt file\n")
            file.write("# Format: table_name | column_name | [query_name] | query_template\n\n")

            # Write tables and columns
            for table, column in tables_columns:
                file.write(f"{table} | {column}\n")

            # Write queries
            for query_name, query_template in queries:
                file.write(f"query | {query_name} | {query_template}\n")

        print(f"Configuration file '{config_path}' generated successfully.")

    except Exception as e:
        print(f"Error generating config file: {str(e)}")

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == '__main__':
    generate_config()
