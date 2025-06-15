import pandas as pd
import json

df = pd.read_csv("data/movies_raw.csv")

# Rename columns
df.rename(columns={
    "vote_average": "rating",
    "genre_ids": "genres"
}, inplace=True)

# Convert genre string to valid JSON format
df["genres"] = df["genres"].apply(lambda g: json.dumps(json.loads(g)))

# Reorder to match table column order
df = df[[
    "movie_id", "title", "overview", "genres",
    "rating", "vote_count", "popularity", "release_date"
]]

# Save cleaned version
df.to_csv("data/movies_cleaned.csv", index=False)
