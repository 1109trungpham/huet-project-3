import duckdb
from pathlib import Path

path = Path("/Users/trung/desktop/huet-project-3/data/clean_data.parquet")

# Truy vấn trực tiếp từ file Parquet cực kỳ chuyên nghiệp bằng DuckDB

data = duckdb.sql(f"SELECT * FROM '{path}' LIMIT 10").fetchdf()
print(data)



# Kiểm tra các case mâu thuẫn
conflicts = duckdb.sql(f"""
    SELECT star, vader_score, review_full 
    FROM '{path}' 
    WHERE is_conflict = True 
    LIMIT 5
""").fetchdf()
print(conflicts)