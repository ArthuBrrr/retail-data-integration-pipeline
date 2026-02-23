import duckdb
from pathlib import Path

# Base project directory
BASE = Path(__file__).resolve().parent.parent

# Paths
db_path = BASE / "data_lake" / "warehouse" / "stats.duckdb"
output_dir = BASE / "outputs"
output_dir.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(str(db_path))

# --------------------------------------------------
# 1) Availability of January 2026
# --------------------------------------------------
df_q1 = con.execute("""
    SELECT
        CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END AS january_2026_available,
        COUNT(*) AS rows_found
    FROM france_monthly_real_estate
    WHERE annee_mois = DATE '2026-01-01';
""").df()

df_q1.to_csv(output_dir / "q1_january_2026_availability.csv", index=False)

# --------------------------------------------------
# 2) Latest available month
# --------------------------------------------------
df_q2 = con.execute("""
    SELECT MAX(annee_mois) AS latest_available_month
    FROM france_monthly_real_estate;
""").df()

df_q2.to_csv(output_dir / "q2_latest_available_month.csv", index=False)

# --------------------------------------------------
# 3) Median price per m² (France)
# --------------------------------------------------
df_q3 = con.execute("""
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
        f.med_prix_m2_maison AS median_price_m2_houses
    FROM france_monthly_real_estate f
    JOIN target_month t ON f.annee_mois = t.month;
""").df()

df_q3.to_csv(output_dir / "q3_median_price_france.csv", index=False)

# --------------------------------------------------
# 4) Price evolution vs same month previous year
# --------------------------------------------------
df_q4 = con.execute("""
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
        current.med_prix_m2_appartement - previous.med_prix_m2_appartement AS diff_apt,
        100.0 * (current.med_prix_m2_appartement - previous.med_prix_m2_appartement)
              / NULLIF(previous.med_prix_m2_appartement, 0) AS pct_apt,

        current.med_prix_m2_maison AS cur_house,
        previous.med_prix_m2_maison AS prev_house,
        current.med_prix_m2_maison - previous.med_prix_m2_maison AS diff_house,
        100.0 * (current.med_prix_m2_maison - previous.med_prix_m2_maison)
              / NULLIF(previous.med_prix_m2_maison, 0) AS pct_house
    FROM current
    LEFT JOIN previous ON TRUE;
""").df()

df_q4.to_csv(output_dir / "q4_price_evolution_year_over_year.csv", index=False)

# --------------------------------------------------
# 5a) Top 10 departments by number of transactions
# --------------------------------------------------
df_q5a = con.execute("""
    SELECT
        code_geo,
        libelle_geo,
        nb_ventes_apt_maison AS total_transactions
    FROM departement_bi
    ORDER BY total_transactions DESC
    LIMIT 10;
""").df()

df_q5a.to_csv(output_dir / "q5a_top10_departments_transactions.csv", index=False)

# --------------------------------------------------
# 5b) Top 10 departments by median price per m²
# --------------------------------------------------
df_q5b = con.execute("""
    SELECT
        code_geo,
        libelle_geo,
        med_prix_m2_apt_maison AS median_price_m2
    FROM departement_bi
    WHERE med_prix_m2_apt_maison IS NOT NULL
    ORDER BY median_price_m2 DESC
    LIMIT 10;
""").df()

df_q5b.to_csv(output_dir / "q5b_top10_departments_price.csv", index=False)

print("✅ BI results exported to outputs/")
