# etudiant_datagouv_fr

Ce projet vise à mettre en place un pipeline complet d'ingénierie de données pour l'analyse des effectifs d'étudiants inscrits dans l'enseignement supérieur en France. Le processus couvre l'extraction des données depuis une source gouvernementale (data.gouv.fr), la transformation, le chargement dans un data warehouse Snowflake, l'orchestration avec Apache Airflow, et la visualisation des résultats.

# Architecture du Projet
Extraction des Données :

Utilisation d'une fonction Python pour extraire les données depuis l'API gouvernementale. Les données sont stockées dans un fichier CSV.
Transformation des Données :

Apache Airflow orchestre le pipeline de transformation. Les données sont nettoyées, sélectionnées, et enrichies, notamment avec des informations géographiques.
Chargement dans le Data Warehouse :

Les données transformées sont chargées dans un data warehouse Snowflake à l'aide de Snowpipe. Une structure de table claire est définie pour faciliter les analyses.
Orchestration avec Apache Airflow :

Airflow planifie et exécute les différentes étapes du pipeline de manière automatisée et régulière.
Visualisations et KPIs :

Les résultats sont présentés via des tableaux de bord interactifs, fournissant une vue détaillée des effectifs étudiants.
![archidata drawio](https://github.com/Oiver237/etudiant_datagouv_fr/assets/73575249/55d98f8b-9d90-441c-8628-bbc0df4ecbfb)
