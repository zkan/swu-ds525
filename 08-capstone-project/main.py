import psycopg2


def main():
    host = "redshift-cluster-1.cwryxvpilhye.us-east-1.redshift.amazonaws.com"
    dbname = "dev"
    user = "awsuser"
    password = ""
    port = "5439"
    conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()
    
    # Drop table if it exists
    drop_table_query = "DROP TABLE IF EXISTS employees"
    cur.execute(drop_table_query)
    conn.commit()

    # Create table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS employees (
        name text,
        salary int
    )
    """
    cur.execute(create_table_query)
    conn.commit()

    # Copy data from S3 to the table we created above
    copy_table_query = """
    COPY employees FROM 's3://zkan-swu-labs/employees.csv'
    ACCESS_KEY_ID ''
    SECRET_ACCESS_KEY ''
    SESSION_TOKEN ''
    CSV
    IGNOREHEADER 1
    REGION 'us-east-1'
    """
    cur.execute(copy_table_query)
    conn.commit()

    conn.close()


if __name__ == "__main__":
    main()
