import streamlit as st
import snowflake.connector
from dotenv import load_dotenv
import os

# Load credentials
load_dotenv()

# Streamlit UI
st.set_page_config(page_title="Mood-Based Movie Recommender 🎬", layout="centered")
st.title("🎭 Mood-Based Movie Recommender")
st.write("Pick your current mood to get top movie recommendations!")

# Mood selection
mood = st.selectbox(
    "Select your mood", ["Happy", "Sad", "Excited", "Scared", "Curious", "Nostalgic"]
)


# Connect to Snowflake
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )


conn = get_connection()
cursor = conn.cursor()


# Log mood input - use Snowflake binding style
def log_mood(mood):
    try:
        cursor.execute("INSERT INTO mood_input_log (mood) VALUES (%s)", (mood,))
    except Exception as e:
        st.warning(f"⚠️ Could not log mood: {e}")


# Fetch top movies using the table function get_top_movies
def fetch_recommendations(mood):
    try:
        query = f"SELECT * FROM TABLE(get_top_movies('{mood}'))"
        cursor.execute(query)
        return cursor.fetchall()
    except Exception as e:
        st.error(f"❌ Failed to fetch recommendations: {e}")
        return []


if st.button("🎬 Show Recommendations"):
    log_mood(mood)
    results = fetch_recommendations(mood)
    if results:
        st.success(f"Top movie picks for **{mood}** mood:")
        for idx, (title, rating) in enumerate(results, start=1):
            st.markdown(f"**{idx}. {title}** — ⭐ {rating}")
    else:
        st.info("No recommendations found.")
