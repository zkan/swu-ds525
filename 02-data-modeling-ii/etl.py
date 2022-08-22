from cassandra.cluster import Cluster


cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

# Create keyspace
try:
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS github_events
        WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
        """
    )
except Exception as e:
    print(e)

# Set keyspace
try:
    session.set_keyspace("github_events")
except Exception as e:
    print(e)

query = """
CREATE TABLE IF NOT EXISTS table_name
(
    x int,
    y int,
    z text,
    PRIMARY KEY (
        x,
        y
    )
)
"""
try:
    session.execute(query)
except Exception as e:
    print(e)


# Read files then insert data into table
query = """
INSERT INTO table_name (x, y, z) VALUES (%s, %s, %s)
"""
session.execute(query, 1, 2, 3)


query = "SELECT xyz from table_name WHERE x = 1 AND y = 2"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print(row)

# Drop table
query = "drop table table_name"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
