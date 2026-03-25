import pandas as pd
from pathlib import Path

path = Path('/Users/trung/desktop/huet-project-3/data/clean_data.parquet')

if path.exists():
    table = pd.read_parquet(path)
else:
    raise FileNotFoundError("Error.")

print(table.head(20))