import mysql.connector
import psycopg2

# MySQL connection parameters
mysql_host = "localhost"
mysql_database = "emaildb"
mysql_user = "root"
mysql_password = "root"

# PostgreSQL connection parameters
postgres_host = "localhost"
postgres_database = "learn1"
postgres_user = "postgres"
postgres_password = "roots"

try:
    # Establish connections to both databases
    mysql_conn = mysql.connector.connect(
        host=mysql_host,
        database=mysql_database,
        user=mysql_user,
        password=mysql_password
    )
    postgres_conn = psycopg2.connect(
        host=postgres_host,
        database=postgres_database,
        user=postgres_user,
        password=postgres_password
    )

    # Get a list of tables in the MySQL database
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute("SHOW TABLES")
    tables = [row[0] for row in mysql_cursor.fetchall()]

    # Migrate each table
    for table in tables:
        # Get the table structure
        mysql_cursor.execute(f"DESCRIBE {table}")
        table_structure = mysql_cursor.fetchall()

        # Create the table in PostgreSQL
        postgres_cursor = postgres_conn.cursor()
        create_table_query = f"CREATE TABLE {table} ("
        for column in table_structure:
            create_table_query += f"{column[0]} {column[1]}, "
        create_table_query = create_table_query[:-2] + ");"
        postgres_cursor.execute(create_table_query)

        # Get the data from the MySQL table
        mysql_cursor.execute(f"SELECT * FROM {table}")
        data = mysql_cursor.fetchall()

        # Insert the data into the PostgreSQL table
        insert_query = f"INSERT INTO {table} VALUES ("
        for _ in range(len(table_structure)):
            insert_query += "%s, "
        insert_query = insert_query[:-2] + ");"
        postgres_cursor.executemany(insert_query, data)

        # Commit changes
        postgres_conn.commit()

    print("Migration complete!")

except mysql.connector.Error as e:
    print(f"MySQL error: {e}")

except psycopg2.Error as e:
    print(f"PostgreSQL error: {e}")

finally:
    if 'mysql_conn' in locals():
        mysql_conn.close()
    if 'postgres_conn' in locals():
        postgres_conn.close()
