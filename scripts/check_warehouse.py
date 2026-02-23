import duckdb
from pathlib import Path
BASE = Path(__file__).resolve().parent.parent
db_path = BASE / "data_lake/warehouse/stats.duckdb"
con = duckdb.connect(str(db_path))

print( "Tables in the warehouse:" )
print(con.execute("SHOW TABLES").fetchdf())
print( " Row count for 'titles_clean':" )

print(con.execute("SELECT COUNT(*) FROM titles_clean;").fetchone()[0])

print("Row count country_summary:")
print(con.execute("SELECT COUNT(*) FROM country_summary;").fetchone()[0])   