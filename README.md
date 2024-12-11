# Airflow Pipeline Project

Ce projet contient un pipeline Airflow pour analyser les subventions des associations parisiennes.

## Prérequis

- **Docker** et **Docker Compose** doivent être installés sur votre machine.
- Une connexion internet est requise pour accéder à l'API OpenData Paris.
- **kubectl** doit être installé pour interagir avec les pods Kubernetes.

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

## Vérification des données dans PostgreSQL

Une fois le pipeline exécuté avec succès, vous pouvez vérifier les données insérées dans la base PostgreSQL :

1. Accédez au pod PostgreSQL via kubectl :
   ```bash
   kubectl exec -it <nom_du_pod_postgres> -- psql -U airflow -d airflow
   ```

   Remplacez `<nom_du_pod_postgres>` par le nom de votre pod PostgreSQL (vous pouvez utiliser `kubectl get pods` pour lister les pods).

2. Exécutez une requête SQL pour voir les données :
   ```sql
   SELECT * FROM subventions LIMIT 10;
   ```

3. Vérifiez le nombre total de lignes insérées :
   ```sql
   SELECT COUNT(*) FROM subventions;
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

### Interagir avec PostgreSQL

- Accéder à PostgreSQL :
  ```bash
  kubectl exec -it <nom_du_pod_postgres> -- psql -U airflow -d airflow
  ```

- Lister les tables dans PostgreSQL :
  ```sql
  \dt
  ```

- Voir la structure de la table `subventions` :
  ```sql
  \d subventions
  ```

### Gestion des pods Kubernetes

- Lister tous les pods :
  ```bash
  kubectl get pods
  ```

- Voir les détails d'un pod spécifique :
  ```bash
  kubectl describe pod <nom_du_pod>
  ```

- Redémarrer un service ou un pod :
  ```bash
  kubectl delete pod <nom_du_pod>
  ```
  Kubernetes redéploiera automatiquement un nouveau pod.

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

- **Pipeline bloqué ou en échec** :
  Vérifiez les logs des tâches dans l'interface Airflow ou en utilisant les commandes kubectl mentionnées ci-dessus.

## Licence

Ce projet est sous licence MIT. Consultez le fichier LICENSE pour plus d'informations.

