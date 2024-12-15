# Airflow Pipeline Project

Ce projet contient un pipeline Airflow pour analyser les subventions des associations parisiennes.  
Ce projet s'inscrit dans le module de OLAP du Master 2 IMSD de l'université de d'Evry-Paris Saclay.  
  
Projet réalisé par :
- DIEDHIOU Mamadou
- HAOUD Anas
- NGETH Laurent
- PAZHITNOV Artemii

## Prérequis

- **Docker** et **Docker Compose** doivent être installés sur votre machine.
- **Minikube** et **kubectl** doit être intallé pour interagir avec les pods Kubernetes.
- Une connexion internet est requise pour accéder à l'API OpenData Paris.

## Installation

1. Clonez le projet :
   ```bash
   git clone https://github.com/<votre_nom_utilisateur>/<nom_du_projet>.git
   cd <nom_du_projet>
   ```

### Version Docker-composer

Lancez les conteneurs avec Docker Compose :
  ```bash
  cd airflow-docker/docker-version
  docker-compose up -d
  ```

### Version Kubernetes

1. Lancez le minikube :
  ```bash
  # Pathdags à changer : Mettre le lien ABSOLU du fichier dags du projet, exemple ci-dessous
  pathdags="C:\Users\lngeth\OneDrive\Bureau\Cours\3A\Projet-Olap\airflow-docker\dags"
  minikube start --mount --mount-string="$pathdags:/home/airflow/dags"
  ```

2. Créer les dossiers logs et mettre les droits :
  ```bash
  minikube ssh
  sudo mkdir /home/airflow/logs
  sudo chmod 777 /home/airflow/logs
  exit
  ```

3. Lancez les **pods** Kubernetes :
  ```bash
  cd airflow-docker/k8s
  kubectl apply -f airflow-config.yaml
  kubectl apply -f postgres-deployment.yaml
  kubectl apply -f airflow-deployment.yaml
  ```

4. **Exposez les ports** du Webserver Airflow et le Postgres dans 2 terminals différents
  ```bash
  # Terminal 1
  kubectl port-forward svc/airflow 8080:8080

  # Terminal 2
  kubectl port-forward svc/postgres 5432:5432
  ```

Dans le cas où un des ports est déjà utilisé :
  ```bash
  # Exemple avec port 5432
  netstat -aon | findstr :5432 # pour avoir le PID du processus utilisant le port
  taskkill /PID <1234> /F # avec <1234> le numéro PID du processus
  ```

## Lancer la pipeline Airflow

1. Accédez à l'interface Airflow :
   - URL : [http://localhost:8080](http://localhost:8080)
   - Identifiants par défaut :
     - **Utilisateur** : `admin`
     - **Mot de passe** : `admin`

2. Activez le DAG `subventions_pipeline` dans l'interface Airflow.

3. Lancez une exécution manuelle du DAG si nécessaire.

## Structure du projet

- **dags/** : Contient les scripts Airflow, notamment `subventions_pipeline.py`.
- **docker-version/docker-compose.yaml** : Configuration Docker Compose pour Airflow et Postgres.
- **k8s/...-(deployment|config).yaml** : Fichier de déploiement et configuration des poids Kubernetes.
- **data/** : Répertoire pour les fichiers CSV générés si besoin.
- **logs/** : Répertoire pour les logs générés par Airflow.
- **plugins/** : Répertoire pour ajouter des plugins Airflow (si nécessaire).

## Fonctionnement

Le pipeline est composé de trois étapes principales :
1. **Collecte des données** : Récupère les données via l'API OpenData Paris.
2. **Prétraitement des données** : Nettoie et transforme les données pour l'analyse.
3. **Stockage dans PostgreSQL** : Insère les données prétraitées dans une base de données PostgreSQL avec PySpark.

Les résultats peuvent être consultés directement dans PostgreSQL ou visualisé via un **dashboard PowerBI** que nous avons créer.

## Vérification des données dans PostgreSQL

Une fois le pipeline exécuté avec succès, vous pouvez vérifier les données insérées dans la base PostgreSQL :

1. Accédez au pod PostgreSQL via kubectl :
   ```bash
   kubectl exec -it <nom_du_pod_postgres> -- psql -U airflow -d airflow
   ```

   Remplacez `<nom_du_pod_postgres>` par le nom du pod PostgreSQL (vous pouvez utiliser `kubectl get pods` pour lister les pods car ces noms changent tout le temps).

2. Exécutez une requête SQL pour voir les données :
   ```sql
   SELECT * FROM subventions_cleaned LIMIT 10;
   ```

3. Vérifiez le nombre total de lignes insérées :
   ```sql
   SELECT COUNT(*) FROM subventions_cleaned;
   ```

## Commandes utiles pour le projet

### Gestion des DAGs Airflow

- Tester une tâche spécifique :
  ```bash
  kubectl exec -it <nom_du_pod_airflow> -c webserver -- airflow tasks test subventions_pipeline <nom_tâche> <date>
  ```
  Exemple :
  ```bash
  kubectl exec -it airflow-<id_pod> -c webserver -- airflow tasks test subventions_pipeline store_to_postgresql 2024-12-10T00:50:00
  ```

- Vérifier les logs d'Airflow :
  ```bash
  kubectl exec -it <nom_du_pod_airflow> -c webserver -- cat /opt/airflow/logs/<nom_dag>/<nom_tâche>/<date>/1.log
  ```

- Copier un fichier DAG vers le conteneur Airflow :
  ```bash
  kubectl cp <chemin_du_fichier> <nom_du_pod_airflow>:/opt/airflow/dags/ -c webserver
  ```