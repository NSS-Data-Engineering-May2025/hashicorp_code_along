import duckdb

conn = duckdb.connect(database="C:/Users/kmcke/OneDrive/Desktop/Data_Enginnering/lake_duckdb/ducklake.db")
conn.execute("INSTALL 'ducklake'")
conn.execute("LOAD 'ducklake'")

conn.execute("ATTACH 'ducklake:C:/Users/kmcke/OneDrive/Desktop/Data_Enginnering/lake_duckdb/catalog.duckdb' AS my_lake (DATA_PATH 'C:/Users/kmcke/OneDrive/Desktop/Data_Enginnering/lake_duckdb/data')")
conn.execute("USE my_lake")

conn.execute("CREATE TABLE IF NOT EXISTS currency AS SELECT * FROM 'currency.csv'")

conn.close()