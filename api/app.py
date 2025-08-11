import duckdb
import os
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, HTTPException, Query
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = FastAPI(title="DuckDB Lake API", description="API for interacting with DuckDB Lake")

# Configuration - you can put these in your .env file if you want
DB_PATH = os.getenv("DB_PATH", "/app/lake_duckdb/ducklake.db")
CATALOG_PATH = os.getenv("CATALOG_PATH", "/app/lake_duckdb/catalog.duckdb")
DATA_PATH = os.getenv("DATA_PATH", "/app/lake_duckdb/data")

def get_db_connection():
    """Create a DuckDB connection with lake setup"""
    try:
        # Create directories if they don't exist
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        os.makedirs(os.path.dirname(CATALOG_PATH), exist_ok=True)
        os.makedirs(DATA_PATH, exist_ok=True)
        
        # Create database connection (will create file if it doesn't exist)
        conn = duckdb.connect(database=DB_PATH)
        
        # Try to set up ducklake, but don't fail if it's not available
        try:
            conn.execute("INSTALL ducklake")
            conn.execute("LOAD ducklake")
            conn.execute(f"ATTACH 'ducklake:{CATALOG_PATH}' AS my_lake (DATA_PATH '{DATA_PATH}')")
            conn.execute("USE my_lake")
            print("Successfully set up ducklake")
        except Exception as lake_error:
            # If ducklake setup fails, just use regular DuckDB with CSV data
            print(f"Warning: Could not set up ducklake: {lake_error}")
            print("Setting up regular DuckDB with CSV data instead")
            
            # Check if currency.csv exists and load it
            csv_path = "/app/currency.csv"
            if os.path.exists(csv_path):
                conn.execute("CREATE TABLE IF NOT EXISTS currency AS SELECT * FROM read_csv_auto('/app/currency.csv')")
                print("Loaded currency data from CSV")
            else:
                print("No CSV file found, creating empty database")
        
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

@app.get("/query-ducklake")
async def query_ducklake(
    query: str,
    limit: Optional[int] = Query(100, ge=1, le=1000),
    offset: Optional[int] = Query(0, ge=0)
):
    """Execute a SQL query against the DuckDB Lake"""
    conn = None
    try:
        conn = get_db_connection()
        
        # Add LIMIT and OFFSET to the query
        full_query = f"{query} LIMIT {limit} OFFSET {offset}"
        result = conn.execute(full_query).fetchall()
        
        return {
            "data": result,
            "query": full_query,
            "row_count": len(result)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()

@app.get("/health")
async def health_check():
    """Check if the database is accessible"""
    conn = None
    try:
        conn = get_db_connection()
        conn.execute("SELECT 1")
        return {
            "status": "healthy", 
            "timestamp": datetime.utcnow().isoformat(),
            "database": "connected"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database health check failed: {str(e)}")
    finally:
        if conn:
            conn.close()

@app.get("/tables")
async def list_tables():
    """Get a list of all tables in the database"""
    conn = None
    try:
        conn = get_db_connection()
        result = conn.execute("SHOW TABLES").fetchall()
        tables = [row[0] for row in result]
        return {"tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()

# Optional: Run the server directly with python
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)