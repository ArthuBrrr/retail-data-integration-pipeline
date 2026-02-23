import pandas as pd 
raw = "../data_lake/raw/stats_dvf.csv"
out = "../data_lake/staging/stats_dvf_staging.csv"
df = pd.read_csv(raw)

for col in ["code_geo", "libelle_geo", "echelle_geo", "code_parent"]:
    df[col] = df[col].astype("string").str.strip()
df["echelle_geo"] = df["echelle_geo"].str.lower()

df= df.drop_duplicates(subset= ["code_geo", "annee_mois"])
df["annee_mois"] = pd.to_datetime(df["annee_mois"], format="%Y-%m")
df["annee"] = df["annee_mois"].dt.year
df["mois"] = df["annee_mois"].dt.month

keep = [
    "code_geo", "libelle_geo", "echelle_geo", "code_parent",
    "annee_mois", "annee", "mois",
    "nb_ventes_maison", "med_prix_m2_maison",
    "nb_ventes_appartement", "med_prix_m2_appartement",
    "nb_ventes_apt_maison", "med_prix_m2_apt_maison",
]

df.to_csv(out, index=False)
print("Saved to STAGING:", out)

raw2 = "../data_lake/raw/stats_whole_period.csv"
out2 = "../data_lake/staging/stats_whole_period_staging.csv"
df2 = pd.read_csv(raw2)

for col in ["code_geo", "libelle_geo", "echelle_geo", "code_parent"]:
    df2[col] = df2[col].astype("string").str.strip()
df2["echelle_geo"] = df2["echelle_geo"].str.lower()

df2= df2.drop_duplicates(subset= ["code_geo"])

keep2 = [
    "code_geo", "libelle_geo", "echelle_geo", "code_parent",
    "nb_ventes_maison", "med_prix_m2_maison",
    "nb_ventes_appartement", "med_prix_m2_appartement",
    "nb_ventes_apt_maison", "med_prix_m2_apt_maison",
]

df2.to_csv(out2, index=False)
print("Saved to STAGING:", out2)