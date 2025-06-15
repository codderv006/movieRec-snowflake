import os
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)

cs = conn.cursor()

try:
    print("🔹 Creating internal stage if not exists...")
    cs.execute("CREATE OR REPLACE STAGE movie_stage")

    print("📤 Uploading file to Snowflake stage...")
    cs.execute("PUT file://data/movies_cleaned.csv @movie_stage OVERWRITE = TRUE")

    print("📥 Loading data into movies_raw table...")
    cs.execute(
        """
        COPY INTO movies_raw
        FROM @movie_stage/movies_raw.csv
        FILE_FORMAT = (
            TYPE = 'CSV',
            FIELD_OPTIONALLY_ENCLOSED_BY = '"',
            SKIP_HEADER = 1,
            DATE_FORMAT = 'YYYY-MM-DD'
        )
        ON_ERROR = 'CONTINUE'
    """
    )

    print("✅ Data successfully loaded to Snowflake.")

finally:
    cs.close()
    conn.close()
