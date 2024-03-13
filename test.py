import psycopg2
import psycopg2.extras as extras
import pandas as pd

db_params = {
    'host': 'localhost',
    'dbname': 'dns',
    'user': 'postgres',
    'password': '89242604125.Cc'
}


def execute_values(conn, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]

    cols = ','.join(list(df.columns))

    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()


def dtype_mapping(column_data):
    # Map pandas data types to PostgreSQL data types
    mapping = {
        pd.api.types.is_bool_dtype: 'boolean',
        pd.api.types.is_integer_dtype: 'integer',
        pd.api.types.is_float_dtype: 'numeric',
        pd.api.types.is_string_dtype: f'varchar({column_data.astype(str).apply(len).max()})',
        pd.api.types.is_categorical_dtype: f'varchar({column_data.astype(str).apply(len).max()})',
        # pd.api.types.is_datetime64_any_dtype: 'timestamp without time zone'
    }

    for is_type, pg_type in mapping.items():
        if is_type(column_data):
            return pg_type

    return 'text'


def create_table_from_csv(conn, csv_file, table_name):
    # Load data into the table
    data = pd.read_csv(csv_file)
    data = data.drop('Unnamed: 0', axis=1)

    # Check if table exists
    cursor = conn.cursor()
    cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')")
    table_exists = cursor.fetchone()[0]
    cursor.close()

    # If table does not exist, create it
    if not table_exists:
        columns = ','.join([f'{col} {dtype_mapping(data[col])}' for col in data.columns])
        create_table_sql = f"CREATE TABLE {table_name} ({columns})"
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        conn.commit()
        cursor.close()

    # Load data into the table
    execute_values(conn, data, table_name)


if __name__ == "__main__":
    # Establishing connection
    conn = psycopg2.connect(**db_params)

    # Table creation and data insertion for each CSV file
    csv_files_info = [
        {'file': 'data/t_branches.csv', 'table_name': 'branches'},
        {'file': 'data/t_cities.csv', 'table_name': 'cities'},
        {'file': 'data/t_products.csv', 'table_name': 'products'},
        {'file': 'data/t_sales.csv', 'table_name': 'sales'}
    ]

    for csv_file_info in csv_files_info:
        csv_file_path = csv_file_info['file']
        table_name = csv_file_info['table_name']

        # Check if file exists
        if not pd.io.common.file_exists(csv_file_path):
            print(f"File {csv_file_path} not found.")
            continue

        print(f"Creating table {table_name} from {csv_file_path}")
        create_table_from_csv(conn, csv_file_path, table_name)

    # Close the connection
    conn.close()
