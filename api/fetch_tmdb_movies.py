import os
import requests
import json
import time
import pandas as pd
from dotenv import load_dotenv

# Load .env credentials
load_dotenv()
API_KEY = os.getenv("TMDB_API_KEY")

# Constants
BASE_URL = "https://api.themoviedb.org/3/movie/popular"
LANGUAGE = "en-US"
TOTAL_PAGES = 50  # 20 movies per page = ~1000 movies
OUTPUT_PATH = "data/movies_raw.csv"

# Ensure output directory exists
os.makedirs("data", exist_ok=True)

# Setup session with headers
session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})

# Collect data
movies_data = []
retry_delay = 5  # Initial retry delay

for page in range(1, TOTAL_PAGES + 1):
    print(f">> Fetching page {page}...")

    try:
        response = session.get(
            BASE_URL,
            params={"api_key": API_KEY, "language": LANGUAGE, "page": page},
            timeout=10,
            verify=True,  # Enable SSL cert verification
        )

        if response.status_code == 429:
            print("⚠️  Rate limit hit. Sleeping 10 seconds...")
            time.sleep(10)
            continue
        elif response.status_code != 200:
            print(f"❌ Failed page {page} — Status code {response.status_code}")
            continue

        results = response.json().get("results", [])

        for movie in results:
            movies_data.append(
                {
                    "movie_id": movie.get("id"),
                    "title": movie.get("title"),
                    "overview": movie.get("overview"),
                    "vote_average": movie.get("vote_average"),
                    "vote_count": movie.get("vote_count"),
                    "popularity": movie.get("popularity"),
                    "release_date": movie.get("release_date"),
                    "genre_ids": json.dumps(
                        movie.get("genre_ids")
                    ),  # ← NO extra wrapping quotes
                }
            )

        time.sleep(0.5)  # Be kind to TMDB
        retry_delay = 5  # Reset retry delay on success

    except requests.exceptions.RequestException as e:
        print(f"⚠️  Error on page {page}: {e}")
        print(f"⏳ Retrying after {retry_delay} seconds...")
        time.sleep(retry_delay)
        retry_delay = min(retry_delay * 2, 60)  # Exponential backoff (max 60s)
        continue

# Save to CSV
df = pd.DataFrame(movies_data)
df.to_csv(OUTPUT_PATH, index=False)
print(f"✅ Data saved to {OUTPUT_PATH}")
