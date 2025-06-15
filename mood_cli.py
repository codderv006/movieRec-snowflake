import snowflake.connector

# Connect to Snowflake
conn = snowflake.connector.connect(
    user="YOUR_USERNAME",
    password="YOUR_PASSWORD",
    account="YOUR_ACCOUNT_ID",
    warehouse="YOUR_WAREHOUSE",
    database="MOVIES_DB",
    schema="RECOMMENDER"
)

mood = input("Enter your mood (e.g., Happy, Sad, Excited): ")

query = f"""
    SELECT title, rating, popularity 
    FROM mood_recommendations 
    WHERE mood = '{mood}' 
    ORDER BY rating DESC 
    LIMIT 10
"""

cursor = conn.cursor()
cursor.execute(query)

print(f"\nTop movie recommendations for mood: {mood}\n")
for row in cursor.fetchall():
    print(f"🎬 {row[0]} — ⭐ {row[1]} — 🔥 {row[2]}")

cursor.close()
conn.close()
