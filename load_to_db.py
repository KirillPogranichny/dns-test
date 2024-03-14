import psycopg2
import psycopg2.extras as extras
import pandas as pd


# Data for connecting to DB
db_params = {
    'database': 'postgres',
    'user': 'postgres',
    'password': 'admin',
    'host': '127.0.0.1',
    'port': '5432'
}


def dtype_mapping(column_data):
    # Map pandas data types to PostgreSQL data types
    mapping = {
        pd.api.types.is_bool_dtype: 'boolean',
        pd.api.types.is_integer_dtype: 'integer',
        pd.api.types.is_float_dtype: 'numeric',
        pd.api.types.is_string_dtype: f'varchar({column_data.astype(str).apply(len).max()})',
        lambda x: isinstance(x, pd.CategoricalDtype): f'varchar({column_data.astype(str).apply(len).max()})',
        pd.api.types.is_datetime64_dtype: 'timestamp without time zone'
    }

    for is_type, pg_type in mapping.items():
        if is_type(column_data):
            return pg_type

    return 'text'


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
    print(f'Data has been inserted into {table} table\n')
    cursor.close()


def create_table_from_csv(conn, csv_file, table_name, primary_key_column=None, foreign_key_columns=None):
    # Load data into the table
    data = pd.read_csv(csv_file)
    data = data.drop('Unnamed: 0', axis=1)

    for col in data.columns:
        if data[col].dtype == 'object' and data[col].str.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$').all():
            data[col] = pd.to_datetime(data[col], format='%Y-%m-%d %H:%M:%S', errors='coerce')

    # Check if table exists
    cursor = conn.cursor()
    cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')")
    table_exists = cursor.fetchone()[0]
    cursor.close()

    # If table does not exist, create it
    if not table_exists:
        columns = [f'{col} {dtype_mapping(data[col])}' for col in data.columns]

        # Creating columns with PK and FK according to data from the dictionary
        if primary_key_column:
            columns.append(f'PRIMARY KEY ({primary_key_column})')
        if foreign_key_columns:
            for column, refs_list in foreign_key_columns.items():
                for refs in refs_list:
                    ref_table, ref_column = refs[0], refs[1]
                    columns.append(f'FOREIGN KEY ({column}) REFERENCES {ref_table}({ref_column})')

        columns_str = ','.join(columns)
        create_table_sql = f"CREATE TABLE {table_name} ({columns_str})"
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
        {'file': 'data/t_cities.csv', 'table_name': 'cities', 'primary_key_column': 'Ссылка'},
        {'file': 'data/t_branches.csv', 'table_name': 'branches',
         'primary_key_column': 'Ссылка',
         'foreign_key_columns': {'Город': [('cities', 'Ссылка')]}},
        {'file': 'data/t_products.csv', 'table_name': 'products',
         'primary_key_column': 'Ссылка'},
        {'file': 'data/t_sales.csv', 'table_name': 'sales',
         'foreign_key_columns': {'Филиал': [('branches', 'Ссылка')],
                                 'Номенклатура': [('products', 'Ссылка')]}}
    ]

    for csv_file_info in csv_files_info:
        csv_file_path = csv_file_info['file']
        table_name = csv_file_info['table_name']
        primary_key_column = csv_file_info.get('primary_key_column')
        foreign_key_columns = csv_file_info.get('foreign_key_columns')

        # Check if file exists
        if not pd.io.common.file_exists(csv_file_path):
            print(f"File {csv_file_path} not found.")
            continue

        print(f"Creating table {table_name} from {csv_file_path}")
        create_table_from_csv(conn, csv_file_path, table_name, primary_key_column, foreign_key_columns)

    # Close the connection
    conn.close()
