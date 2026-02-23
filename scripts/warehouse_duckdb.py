import duckdb
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
france_estate = BASE / "data_lake/curated/france_monthly_real_estate.csv"
departement_estate = BASE / "data_lake/curated/departement_real_estate.csv"
db_path = BASE / "data_lake/warehouse/stats.duckdb"
db_path.parent.mkdir(parents=True, exist_ok=True)
con = duckdb.connect(str(db_path))
con.execute("CREATE OR REPLACE TABLE france_monthly_real_estate AS SELECT * FROM read_csv_auto(?)",[str(france_estate)]
)
con.execute("CREATE OR REPLACE TABLE departement_bi AS SELECT *FROM read_csv_auto(?)",[str(departement_estate)]
)
print("Warehouse created:", db_path)