
import mysql.connector
import psycopg2

# MySQL connection parameters
mysql_host = "localhost"
mysql_database = "db1"
mysql_user = "root"
mysql_password = "root"

# PostgreSQL connection parameters
postgres_host = "localhost"
postgres_database = "learn1"
postgres_user = "postgres"
postgres_password = "roots"

# PostgreSQL connection parameters
postgres_host = "ep-odd-dust-a1lipsza.ap-southeast-1.aws.neon.tech"
postgres_database = "verceldb"
postgres_user = "default"
postgres_password = "3lDncJqHNvL6"

try:
    # Establish connections to both databases
    postgres_conn = psycopg2.connect(
        host=postgres_host,
        database=postgres_database,
        user=postgres_user,
        password=postgres_password
    )
    mysql_conn = mysql.connector.connect(
        host=mysql_host,
        database=mysql_database,
        user=mysql_user,
        password=mysql_password
    )

    # Get a list of tables in the PostgreSQL database
    postgres_cursor = postgres_conn.cursor()
    postgres_cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
    tables = [row[0] for row in postgres_cursor.fetchall()]

    # Migrate each table
    for table in tables:
        # Get the table structure
        postgres_cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name='{table}';")
        table_structure = postgres_cursor.fetchall()

        # Create the table in MySQL
        mysql_cursor = mysql_conn.cursor()
        create_table_query = f"CREATE TABLE {table} ("

        for column in table_structure:
            data_type = column[1].lower()

            if data_type == "character varying":
                data_type = "varchar(255)"
            elif data_type == "integer":
                data_type = "int"
            elif data_type == "boolean":
                data_type = "tinyint"
            elif data_type == "timestamp with time zone":
                data_type = "datetime"

            create_table_query += f"{column[0]} {data_type}, "

        create_table_query = create_table_query[:-2] + ");"
        mysql_cursor.execute(create_table_query)

        # Get the data from the PostgreSQL table
        postgres_cursor.execute(f"SELECT * FROM {table};")
        data = postgres_cursor.fetchall()

        # Insert the data into the MySQL table
        insert_query = f"INSERT INTO {table} VALUES ("
        for _ in range(len(table_structure)):
            insert_query += "%s, "
        insert_query = insert_query[:-2] + ");"
        mysql_cursor.executemany(insert_query, data)

        # Commit changes
        mysql_conn.commit()

    print("Migration complete!")

except psycopg2.Error as e:
    print(f"PostgreSQL error: {e}")

except mysql.connector.Error as e:
    print(f"MySQL error: {e}")

finally:
    if 'postgres_conn' in locals():
        postgres_conn.close()
    if 'mysql_conn' in locals():
        mysql_conn.close()


