from flask import Flask, request, send_file, jsonify
import pandas as pd
import zipfile
from io import BytesIO, StringIO
import re
import os
import mysql.connector.pooling
from concurrent.futures import ThreadPoolExecutor, as_completed
import mysql.connector.errors as mysql_errors
import argparse

app = Flask(__name__)

# Database connection settings
host = 'localhost'
user = 'root'
password = ''
port = 3306
database = 'HugeDatabase'

# Connection pool
pool = mysql.connector.pooling.MySQLConnectionPool(
    pool_name="my_pool",
    pool_size=20,
    host=host,
    user=user,
    password=password,
    database=database,
    port=port,
    pool_reset_session=True,
    connect_timeout=30
)

def get_user_friendly_db_error(err):
    if isinstance(err, mysql_errors.InterfaceError):
        return "Database connection failed: Network issue or incorrect host/port configuration"
    elif isinstance(err, mysql_errors.ProgrammingError):
        if "access denied" in str(err).lower():
            return "Database access denied: Invalid username or password"
        return "Database error: Invalid SQL query or parameters"
    elif isinstance(err, mysql_errors.DatabaseError):
        return "Database service unavailable or not running"
    elif isinstance(err, mysql_errors.OperationalError):
        if "unknown database" in str(err).lower():
            return f"Database error: The database '{database}' does not exist"
        elif "connection timed out" in str(err).lower():
            return "Database connection timed out"
        return "Database operation failed: Check your connection settings"
    else:
        return f"Database error: {str(err)}"

def load_table_config(config_path='config.txt'):
    table_config = {}
    queries = []
    try:
        with open(config_path, 'r') as f:
            next(f)
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'): # Skip empty lines and comments
                    continue
                
                parts = [p.strip(' "\'') for p in line.split('|') if p.strip()]
                
                if len(parts) == 2:
                    table, column = parts
                    table_config[table] = column
                elif len(parts) == 3 and parts[0].lower() == 'query':
                    query_name, query_template = parts[1], parts[2]
                    queries.append((query_name, query_template))
                else:
                    print(f"Skipping invalid line in config: {line}")
                    
    except FileNotFoundError:
        return None, ["Configuration error: config.txt file not found"]
    except PermissionError:
        return None, ["Configuration error: No permission to read config.txt"]
    except Exception as e:
        return None, [f"Configuration error: Failed to load config file ({str(e)})"]
    
    return table_config, queries

def generate_query_from_template(template, table_name, column_name, select_columns="*"):
    if not select_columns or select_columns.strip().lower() in ("*", "all"):
        select_clause = "*"
    else:
        columns = [col.strip() for col in select_columns.split(",") if col.strip()]
        if column_name and column_name not in columns:
            columns.insert(0, column_name)
        select_clause = ", ".join(columns)
    
    return template.format(
        select_clause=select_clause,
        table_name=table_name,
        column_name=column_name
    )

def connect_db():
    """Get a database connection from the pool with consistent return type"""
    try:
        connection = pool.get_connection()
        return connection, None  # Always return (connection, error) tuple
    except Exception as err:
        return None, get_user_friendly_db_error(err)

def parse_case_numbers(input_data):
    input_str = str(input_data).strip()
    if not input_str:
        return None, "Please provide at least one case number"
    
    case_numbers = re.split(r'[\s,]+', input_str)
    valid_numbers = [num for num in case_numbers if num.isdigit()]
    
    if not valid_numbers:
        return None, "No valid case numbers found (empty or invalid format)"
    return valid_numbers, None

def execute_case_queries(case_numbers, table_name, column_name, query_name, query_id, select_columns, query_template, max_rows=None):
    connection, connection_error = connect_db()  # Now always unpackable
    execution_log = []
    
    if connection_error:
        error_msg = f"Failed to connect to database: {connection_error}"
        execution_log.append(error_msg)
        return [{"error": error_msg}], execution_log
    
    results = []
    ROW_LIMIT = 10000  # Safety row limit

    try:
        case_numbers_str = ','.join([str(num) for num in case_numbers])
        execution_log.append(f"\n=== Processing Table: {table_name} ===")
        execution_log.append(f"Search Column: {column_name}")
        execution_log.append(f"Requested columns: {select_columns}")
        execution_log.append(f"Case numbers: {case_numbers_str}")
        execution_log.append(f"Using query template: {query_template}")
        
        # Generate query with LIMIT clause if max_rows is specified
        QUERIES = generate_query_from_template(query_template, table_name, column_name, select_columns)
        
        cursor = connection.cursor(prepared=True)
        
        in_values = ','.join([f"'{num}'" for num in case_numbers if num is not None and num != ''])
        actual_query = QUERIES.strip().replace('%s', in_values)
        
        explain_query = f"EXPLAIN {actual_query}"
        
        cursor.execute(explain_query)
        explain_result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        if 'rows' not in [col.lower() for col in columns]:
            raise ValueError("Cannot estimate query performance: EXPLAIN output format unexpected")
        
        rows_idx = [col.lower() for col in columns].index('rows')
        explain_rows = sum(int(row[rows_idx]) or 0 for row in explain_result)
        execution_log.append(f"Estimated rows to scan: {explain_rows}")
        
        if explain_rows > ROW_LIMIT:
            error_msg = (f"Query skipped: This would scan approximately {explain_rows:,} rows "
                        f"(exceeds safety limit of {ROW_LIMIT:,} rows). "
                        "Please refine your search criteria")
            execution_log.append(error_msg)
            results.append({
                "error": error_msg,
                "query_name": query_name,
                "table": table_name,
                "case_numbers": case_numbers,
                "query": actual_query,
                "estimated_rows": explain_rows
            })
            return results, execution_log 
        
        placeholders = ','.join(['%s'] * len(case_numbers))
        main_query = QUERIES.replace('%s', placeholders)
        params = tuple(case_numbers)
            
        cursor.execute(main_query, params)
        result = cursor.fetchall()
        
        if not result:
            execution_log.append("No matching records found")
            results.append({
                "data": None,
                "query_name": query_name,
                "table": table_name,
                "query": main_query,
                "estimated_rows": explain_rows,
                "actual_rows": 0,
                "case_numbers": case_numbers,
                "message": "No matching records found in this table"
            })
        else:
            # Create a DataFrame and filter top N rows per group (case number)
            df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])
            actual_rows = len(df)
            
            if max_rows:
                df = df.groupby(column_name).head(max_rows).reset_index(drop=True)
            
            execution_log.append(f"Successfully retrieved {len(df)} records")
            
            results.append({
                "data": df,
                "query_name": query_name,
                "table": table_name,
                "query": main_query,
                "estimated_rows": explain_rows,
                "actual_rows": len(df),
                "case_numbers": case_numbers,
                "message": f"Found {len(df)} matching records"
            })
    except Exception as e:
        error_msg = "Query execution failed"
        if isinstance(e, mysql_errors.Error):
            error_msg += f": {get_user_friendly_db_error(e)}"
        else:
            error_msg += f": {str(e)}"
            
        execution_log.append(error_msg)
        execution_log.append("Technical details:")
        execution_log.append(str(e))
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
    
    return results, execution_log


def execute_case_queries_batch(case_batch, table_name, column_name, query_name, query_id, select_columns, query_template, max_rows=None):
    """Process a single batch of case numbers"""
    return execute_case_queries(case_batch, table_name, column_name, query_name, query_id, select_columns, query_template, max_rows)

def execute_queries(case_numbers, query_id, select_columns="*", max_rows=None):
    all_results = []
    all_messages = []
    table_config, queries = load_table_config()

    if not table_config or not queries:
        return [], ["Config error: No tables or queries found"]

    # Batch configuration
    BATCH_SIZE = 50
    batches = [case_numbers[i:i + BATCH_SIZE] for i in range(0, len(case_numbers), BATCH_SIZE)]
    
    # Dictionary to accumulate results by table
    table_results = {}
    
    with ThreadPoolExecutor(max_workers=min(os.cpu_count() * 2, 20)) as executor:
        futures = []
        
        for batch_idx, case_batch in enumerate(batches, 1):
            # Process each table with each query template for this batch
            for table_name, column_name in table_config.items():
                for query_name, query_template in queries:
                    futures.append(executor.submit(
                        execute_case_queries_batch,
                        case_batch,
                        table_name,
                        column_name,
                        query_name,
                        query_id,
                        select_columns,
                        query_template,
                        max_rows  # Pass max_rows here
                    ))

        for future in as_completed(futures):
            batch_results, messages = future.result()
            all_messages.extend(messages)
            
            # Accumulate results by table
            for result in batch_results:
                if 'data' not in result or not isinstance(result['data'], pd.DataFrame):
                    continue
                    
                table_name = result['table']
                query_name = result['query_name']
                key = (table_name, query_name)
                
                if key not in table_results:
                    table_results[key] = {
                        'data': [],
                        'query_name': query_name,
                        'table': table_name,
                        'query': result['query'],
                        'estimated_rows': result.get('estimated_rows', 0),
                        'actual_rows': 0,
                        'case_numbers': set(),
                        'message': ''
                    }
                
                # Append the dataframe and update stats
                if result['data'] is not None:
                    table_results[key]['data'].append(result['data'])
                    table_results[key]['actual_rows'] += result.get('actual_rows', 0)
                    table_results[key]['case_numbers'].update(result['case_numbers'])
            
            all_messages.append("=== Batch Completed ===\n")

    # Combine dataframes for each table
    for key in table_results:
        if table_results[key]['data']:
            combined_df = pd.concat(table_results[key]['data'], ignore_index=True)
            table_results[key]['data'] = combined_df.drop_duplicates()
            table_results[key]['message'] = f"Found {len(table_results[key]['data'])} unique records"
        else:
            table_results[key]['data'] = None
    
    # Convert back to list format expected by the rest of the code
    all_results = list(table_results.values())
    
    return all_results, all_messages


@app.route('/execute_queries', methods=['POST'])
def handle_execute_queries():
    case_input = request.form.get('case_number')
    query_id = request.form.get('query_id')
    select_columns = request.form.get('columns', '*')
    max_rows = request.form.get('max_rows', None)

    if not case_input:
        return jsonify({'error': 'Please provide at least one case number'}), 400
    if not query_id:
        return jsonify({'error': 'Missing query identifier (query_id)'}), 400

    case_numbers, error = parse_case_numbers(case_input)
    if error:
        return jsonify({'error': error}), 400

    try:
        max_rows_value = int(max_rows) if max_rows else None
        results, messages = execute_queries(case_numbers, query_id, select_columns=select_columns, max_rows=max_rows_value)

        if not results:
            return jsonify({
                'error': 'No queries were executed',
                'possible_reasons': [
                    'No valid configuration found',
                    'All queries exceeded row limits',
                    'Database connection issues'
                ],
                'log': messages
            }), 404

        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            has_data = False
            for result in results:
                if 'data' not in result or not isinstance(result['data'], pd.DataFrame):
                    continue
                    
                df = result['data']
                if df.empty:
                    continue
                
                file_name = f"{result['query_name']}_{result['table']}_{query_id}.csv"
                df.columns = [str(col) for col in df.columns]
                df.columns = [f"{col}_{idx}" if col in df.columns[:idx] else col 
                            for idx, col in enumerate(df.columns)]
                
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False)
                zip_file.writestr(file_name, csv_buffer.getvalue())
                has_data = True

            if not has_data:
                return jsonify({
                    'error': 'No data found for the given case numbers',
                    'log': messages,
                    'suggestion': 'Try different case numbers or check your query configuration'
                }), 404

            zip_file.writestr('execution_log.txt', "\n".join(messages))

        zip_buffer.seek(0)
        return send_file(
            zip_buffer,
            mimetype='application/zip',
            as_attachment=True,
            download_name=f'query_{query_id}.zip'
        )

    except Exception as e:
        return jsonify({
            'error': "Encountered an unexpected error",
            'details': str(e),
            'suggestion': 'Please try again later',
            'log': messages if 'messages' in locals() else []
        }), 500

def run_server(host='0.0.0.0', port=5000, debug=False):
    """Run the Flask server with specified parameters"""
    print(f"Starting server at http://{host}:{port}")
    print("Available endpoints:")
    print(f"  POST /execute_queries - Submit case numbers to query")
    app.run(host=host, port=port, debug=debug)

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Database Query API Server',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--host', default='0.0.0.0',
                       help='Host interface to bind to')
    parser.add_argument('--port', type=int, default=5000,
                       help='Port to run the server on')
    parser.add_argument('--debug', action='store_true', 
                       help='Enable debug mode')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_arguments()
    run_server(host=args.host, port=args.port, debug=args.debug)