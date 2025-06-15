import os
from dotenv import load_dotenv
import snowflake.connector

# Load credentials
load_dotenv()

# Snowflake connection
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
)

cs = conn.cursor()

# Create DB, Schema, and Table
try:
    cs.execute("CREATE DATABASE IF NOT EXISTS MOVIES_DB;")
    cs.execute("USE DATABASE MOVIES_DB;")
    cs.execute("CREATE SCHEMA IF NOT EXISTS RECOMMENDER;")
    cs.execute("USE SCHEMA RECOMMENDER;")
    
    cs.execute("""
        CREATE OR REPLACE TABLE movies_raw (
            movie_id INT,
            title STRING,
            overview STRING,
            genres VARIANT, 
            rating FLOAT,
            vote_count INT,
            popularity FLOAT,
            release_date DATE
        );
    """)
    print("✅ Snowflake schema and table created.")
finally:
    cs.close()
    conn.close()
