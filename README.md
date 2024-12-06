# Airflow Pipeline Project

Ce projet contient un pipeline Airflow pour analyser les subventions des associations parisiennes.

## Prérequis

- **Docker** et **Docker Compose** doivent être installés sur votre machine.
- Une connexion internet est requise pour accéder à l'API OpenData Paris.

## Installation

1. Clonez le projet :
   ```bash
   git clone https://github.com/<votre_nom_utilisateur>/<nom_du_projet>.git
   cd <nom_du_projet>
   ```

2. Lancez les conteneurs avec Docker Compose :
   ```bash
   docker-compose up -d
   ```

3. Accédez à l'interface Airflow :
   - URL : [http://localhost:8080](http://localhost:8080)
   - Identifiants par défaut :
     - **Utilisateur** : `airflow`
     - **Mot de passe** : `airflow`

4. Activez le DAG `subventions_pipeline` dans l'interface Airflow.

5. Lancez une exécution manuelle du DAG si nécessaire.

## Structure du projet

- **dags/** : Contient les scripts Airflow, notamment `subventions_pipeline.py`.
- **docker-compose.yaml** : Configuration Docker Compose pour Airflow.
- **data/** : Répertoire pour les fichiers CSV générés par le pipeline.
- **logs/** : Répertoire pour les logs générés par Airflow.
- **plugins/** : Répertoire pour ajouter des plugins Airflow (si nécessaire).

## Fonctionnement

Le pipeline est composé de trois étapes principales :
1. **Collecte des données** : Récupère les données via l'API OpenData Paris.
2. **Prétraitement des données** : Nettoie et transforme les données pour l'analyse.
3. **Stockage dans PostgreSQL** : Insère les données prétraitées dans une base de données PostgreSQL.

Les résultats peuvent être consultés directement dans PostgreSQL ou sous forme de fichiers CSV.

## Contributions

1. Faites une branche à partir de `main` :
   ```bash
   git checkout -b ma-branche
   ```

2. Effectuez vos modifications et commitez-les :
   ```bash
   git add .
   git commit -m "Description des modifications"
   ```

3. Poussez votre branche sur le dépôt distant :
   ```bash
   git push origin ma-branche
   ```

4. Ouvrez une **Pull Request** sur GitHub pour demander l'intégration de vos modifications.

## Problèmes courants

- **Accès à l'interface Airflow** :
  Assurez-vous que le port `8080` n'est pas utilisé par un autre service.

- **Problème de dépendances** :
  Si vous ajoutez de nouvelles dépendances Python, mettez-les dans `requirements.txt` et relancez les conteneurs :
  ```bash
  docker-compose build
  docker-compose up -d
  ```



