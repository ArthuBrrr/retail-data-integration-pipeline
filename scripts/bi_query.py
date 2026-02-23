import duckdb
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
db_path = BASE / "data_lake" / "warehouse" / "stats.duckdb"

con = duckdb.connect(str(db_path))

# (optionnel) check tables
print("Tables:")
print(con.execute("SHOW TABLES;").df())

# -------------------------
# 1) Is data available for January 2026?
# -------------------------
print("\n1) Is data available for January 2026?")
print(con.execute("""
    SELECT
        CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END AS january_2026_available,
        COUNT(*) AS rows_found
    FROM france_monthly_real_estate
    WHERE annee_mois = DATE '2026-01-01';
""").df())

# -------------------------
# 2) If not, what is the latest available month?
# -------------------------
print("\n2) Latest available month")
print(con.execute("""
    SELECT MAX(annee_mois) AS latest_available_month
    FROM france_monthly_real_estate;
""").df())

# -------------------------
# Helper: target month = Jan 2026 if exists else latest month
# -------------------------
# We'll reuse this logic in Q3 and Q4.

# -------------------------
# 3) Median price per square meter for apartments and houses (France)
# -------------------------
print("\n3) Median price per m² (France) – apartments & houses")
print(con.execute("""
    WITH target_month AS (
        SELECT
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM france_monthly_real_estate
                    WHERE annee_mois = DATE '2026-01-01'
                )
                THEN DATE '2026-01-01'
                ELSE (SELECT MAX(annee_mois) FROM france_monthly_real_estate)
            END AS month
    )
    SELECT
        f.annee_mois,
        f.med_prix_m2_appartement AS median_price_m2_apartments,
        f.med_prix_m2_maison      AS median_price_m2_houses
    FROM france_monthly_real_estate f
    JOIN target_month t ON f.annee_mois = t.month;
""").df())

# -------------------------
# 4) Evolution of prices vs same month previous year
# -------------------------
print("\n4) Price evolution vs same month of previous year")
print(con.execute("""
    WITH target_month AS (
        SELECT
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM france_monthly_real_estate
                    WHERE annee_mois = DATE '2026-01-01'
                )
                THEN DATE '2026-01-01'
                ELSE (SELECT MAX(annee_mois) FROM france_monthly_real_estate)
            END AS month
    ),
    current AS (
        SELECT *
        FROM france_monthly_real_estate
        WHERE annee_mois = (SELECT month FROM target_month)
    ),
    previous AS (
        SELECT *
        FROM france_monthly_real_estate
        WHERE annee_mois = (SELECT month - INTERVAL '1 year' FROM target_month)
    )
    SELECT
        current.annee_mois AS current_month,
        previous.annee_mois AS previous_year_same_month,

        current.med_prix_m2_appartement AS cur_apt,
        previous.med_prix_m2_appartement AS prev_apt,
        (current.med_prix_m2_appartement - previous.med_prix_m2_appartement) AS diff_apt,
        100.0 * (current.med_prix_m2_appartement - previous.med_prix_m2_appartement)
              / NULLIF(previous.med_prix_m2_appartement, 0) AS pct_apt,

        current.med_prix_m2_maison AS cur_house,
        previous.med_prix_m2_maison AS prev_house,
        (current.med_prix_m2_maison - previous.med_prix_m2_maison) AS diff_house,
        100.0 * (current.med_prix_m2_maison - previous.med_prix_m2_maison)
              / NULLIF(previous.med_prix_m2_maison, 0) AS pct_house
    FROM current
    LEFT JOIN previous ON TRUE;
""").df())

# -------------------------
# 5a) Top 10 departments by number of transactions
# -------------------------
print("\n5a) Top 10 departments by number of transactions")
print(con.execute("""
    SELECT
        code_geo,
        libelle_geo,
        nb_ventes_apt_maison AS total_transactions
    FROM departement_bi
    ORDER BY total_transactions DESC
    LIMIT 10;
""").df())

# -------------------------
# 5b) Top 10 departments by median price per m²
# -------------------------
print("\n5b) Top 10 departments by median price per m²")
print(con.execute("""
    SELECT
        code_geo,
        libelle_geo,
        med_prix_m2_apt_maison AS median_price_m2
    FROM departement_bi
    WHERE med_prix_m2_apt_maison IS NOT NULL
    ORDER BY median_price_m2 DESC
    LIMIT 10;
""").df())
# Close connection