import pandas as pd 

staging = "../data_lake/staging/stats_dvf_staging.csv"

france_monthly_real_estate = "../data_lake/curated/france_monthly_real_estate.csv"
departement_real_estate = "../data_lake/curated/departement_real_estate.csv"

df = pd.read_csv(staging, parse_dates=["annee_mois"])

df_france = df[df["echelle_geo"] == "nation"]

df_france = df_france[
    [
        "annee_mois", "annee", "mois",
        "nb_ventes_maison", "med_prix_m2_maison",
        "nb_ventes_appartement", "med_prix_m2_appartement",
        "nb_ventes_apt_maison", "med_prix_m2_apt_maison",
    ]
]

df_france = df_france.sort_values("annee_mois")

df_france.to_csv(france_monthly_real_estate, index=False)
print("France monthly BI dataset saved")


df_dep = df[df["echelle_geo"] == "departement"]

df_dep = (
    df_dep
    .groupby(["code_geo", "libelle_geo"], as_index=False)
    .agg(
        nb_ventes_apt_maison=("nb_ventes_apt_maison", "sum"),
        med_prix_m2_apt_maison=("med_prix_m2_apt_maison", "median"),
    )
)

df_dep = df_dep.dropna(
    subset=["nb_ventes_apt_maison", "med_prix_m2_apt_maison"]
)

df_dep.to_csv(departement_real_estate, index=False)
print("Department BI dataset saved")
