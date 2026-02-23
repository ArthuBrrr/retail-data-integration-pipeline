# DVF Project

Ce projet a pour objectif de charger, analyser et exploiter des données immobilières issues du fichier DVF (Demande de Valeurs Foncières). Il permet de sélectionner les meilleures données, de les analyser et de réaliser des requêtes analytiques avancées (BigQuery).

## Fonctionnalités principales

1. **Chargement des fichiers DVF**
    - Importation de fichiers CSV contenant les données DVF.
    - Nettoyage et validation des données lors du chargement.

2. **Analyse des données**
    - Statistiques descriptives sur les transactions immobilières (prix, surface, localisation, etc.).
    - Détection et gestion des valeurs aberrantes.
    - Visualisation des tendances du marché immobilier.

3. **Sélection des meilleures données**
    - Filtrage selon des critères personnalisés (localisation, type de bien, période, etc.).
    - Agrégation des données pour obtenir des indicateurs pertinents.

4. **Requêtes analytiques avancées (BigQuery)**
    - Intégration avec Google BigQuery pour l’analyse de grands volumes de données.
    - Exécution de requêtes SQL complexes pour extraire des insights.
    - Génération de rapports et dashboards interactifs.

## Prérequis

- Python 3.x
- Pandas, NumPy, Matplotlib/Seaborn
- Google Cloud SDK (pour BigQuery)
- Fichiers DVF au format CSV

## Installation

1. Cloner le dépôt :
    ```
    git clone <url_du_repo>
    cd dvf_project
    ```

2. Installer les dépendances :
    ```
    pip install -r requirements.txt
    ```

3. Configurer l’accès à Google BigQuery si besoin.

## Utilisation

1. **Charger les fichiers DVF**
    - Placer les fichiers CSV dans le dossier `data/`.
    - Lancer le script de chargement :
      ```
      python scripts/load_data.py
      ```

2. **Analyser les données**
    - Exécuter le script d’analyse :
      ```
      python scripts/analyze_data.py
      ```

3. **Effectuer des requêtes BigQuery**
    - Modifier et lancer les requêtes SQL dans `queries/`.
    - Utiliser le script d’exécution :
      ```
      python scripts/run_bigquery.py
      ```

## Structure du projet
.
├── data/                  # Fichiers DVF au format CSV
├── scripts/               # Scripts Python pour le chargement, l’analyse et BigQuery
│   ├── load_data.py
│   ├── analyze_data.py
│   └── run_bigquery.py
├── queries/               # Requêtes SQL pour BigQuery
├── requirements.txt       # Dépendances Python
├── README.me              # Documentation du projet
└── .gitignore             # Fichiers à ignorer par git
