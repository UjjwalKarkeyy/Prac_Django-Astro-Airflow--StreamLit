execute_sql = """
    CREATE TABLE IF NOT EXISTS yt_comments(
    id INTEGER PRIMARY KEY,
    comment TEXT,
    author TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
"""

insert_sql = """
INSERT INTO yt_comments (id, comment, author, timestamp)
VALUES (%s, %s, %s, %s)
ON CONFLICT (id) DO NOTHING;
"""