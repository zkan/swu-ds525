import psycopg2


table_insert = """
    INSERT INTO users (
        xxx
    ) VALUES (%s)
    ON CONFLICT (xxx) DO NOTHING
"""


def process(cur, conn, filepath):
    # Read JSON files from filepath

    # Insert data into tables

    pass


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
    )
    cur = conn.cursor()

    process(cur, conn, filepath="")

    conn.close()


if __name__ == "__main__":
    main()
