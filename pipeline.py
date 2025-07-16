import duckdb
import pandas as pd
import hashlib
from datetime import datetime
from sentence_transformers import SentenceTransformer
import chromadb
from fastapi import FastAPI
import uvicorn

# Step 1: Connect to DuckDB
con = duckdb.connect('my_database.duckdb')

# Step 2: Define SHA256 function for DuckDB
def sha256_func(x):
    return hashlib.sha256(str(x).encode('utf-8')).hexdigest()

con.create_function('sha256', sha256_func, [str], str)

# Step 3: Load raw data into DuckDB
con.execute("CREATE TABLE raw_data AS SELECT * FROM 'IMDB_Dataset.csv'")

# Step 4: Create cleaned_data table
con.execute("""
CREATE TABLE cleaned_data AS
SELECT 
    ROW_NUMBER() OVER() AS id,
    review,
    sentiment,
    sha256(review) AS source_fingerprint,
    CURRENT_TIMESTAMP AS ingestion_timestamp
FROM raw_data
""")

# Step 5: Load cleaned_data for embedding
df = con.execute("SELECT id, review FROM cleaned_data").df()
reviews = df['review'].tolist()
ids = [str(id) for id in df['id']]

# Step 6: Create embeddings
model = SentenceTransformer('all-MiniLM-L6-v2')
embeddings = model.encode(reviews, show_progress_bar=True)

# Step 7: Store in Chroma
client = chromadb.Client()
collection = client.create_collection(name="imdb_reviews")
collection.add(
    documents=reviews,
    embeddings=embeddings,
    metadatas=[{"id": id} for id in ids],
    ids=ids
)

# Step 8: Create FastAPI app
app = FastAPI()

@app.post("/search")
def search(query: str):
    # Compute query embedding
    query_embedding = model.encode([query])[0]
    
    # Query Chroma
    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=3
    )
    
    # Get ids and distances
    matches = results['ids'][0]
    distances = results['distances'][0]
    
    # Fetch from DuckDB
    int_ids = [int(id) for id in matches]
    db_results = con.execute(f"SELECT review, sentiment, ingestion_timestamp FROM cleaned_data WHERE id IN ({','.join(map(str, int_ids))})").df()
    
    # Add quality_score and source_file
    db_results['quality_score'] = distances
    db_results['source_file'] = "IMDB_Dataset.csv"
    
    # Convert to list of dicts
    output = db_results.to_dict(orient='records')
    
    return output

# Step 9: Run the FastAPI server (uncomment to run)
# if __name__ == "__main__":
