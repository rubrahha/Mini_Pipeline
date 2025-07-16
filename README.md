IMDB Movie Reviews Pipeline
Overview
This pipeline processes the IMDB Movie Reviews Dataset (50,000 rows) to create a traceable data infrastructure. It ingests data into DuckDB, adds a SHA256 fingerprint for traceability, embeds reviews in a Chroma vector database for semantic search, and provides a FastAPI search endpoint.
Dataset

Source: IMDB Dataset of 50K Movie Reviews
Columns:
review: Text of the movie review (TEXT)
sentiment: Sentiment label, "positive" or "negative" (TEXT)


Additional columns in cleaned_data:
id: Unique row identifier (INTEGER)
source_fingerprint: SHA256 hash of the review (TEXT)
ingestion_timestamp: Timestamp when data was ingested (TIMESTAMP)



Schema Inference
DuckDB automatically infers the schema from the CSV file, detecting review as TEXT and sentiment as TEXT. The cleaned_data table is created with explicit column definitions to ensure traceability.
Productionization
To scale this pipeline in a cloud-native environment:

Storage: Use Snowflake instead of DuckDB for scalable data warehousing.
Vector Database: Use Pinecone or Weaviate for managed vector storage and search.
API Deployment: Deploy FastAPI on AWS Lambda or Google Cloud Run for serverless scalability.
Orchestration: Use Apache Airflow or Prefect to schedule ingestion, transformation, and embedding tasks.

Setup Instructions

Install dependencies: pip install duckdb chromadb sentence-transformers fastapi uvicorn
Download the dataset from Kaggle and save as IMDB_Dataset.csv.
Run the Python script to set up DuckDB, compute embeddings, and start the FastAPI server.
Access the search API at http://localhost:8000/search with a POST request containing {"query": "your search query"}.

Notes

The quality_score is the distance from the vector search (lower is better).
Ensure sufficient memory for embedding 50,000 reviews.
