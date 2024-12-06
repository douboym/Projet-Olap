from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import psycopg2

# Fonction 1 : Collecte des données
def collect_data():
    try:
        print("Début de la collecte des données...")
        base_url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/subventions-associations-votees-/records"
        all_records = []
        limit = 100
        offset = 0

        while True:
            url = f"{base_url}?limit={limit}&offset={offset}"
            print(f"Requête envoyée : {url}")
            response = requests.get(url)
            print(f"Statut HTTP : {response.status_code}")

            if response.status_code == 200:
                data = response.json()
                results = data.get('results', [])
                if not results:
                    print("Pas plus de résultats, fin de la collecte.")
                    break

                all_records.extend(results)
                offset += limit
                print(f"Page récupérée avec {len(results)} records. Offset : {offset}")
            else:
                print(f"Erreur lors de la collecte des données : {response.text}")
                break

        print(f"Nombre total de records collectés : {len(all_records)}")
        df = pd.DataFrame(all_records)
        df.to_csv('/opt/airflow/dags/data/subventions_raw.csv', index=False)
        print("Données enregistrées avec succès.")

    except Exception as e:
        print(f"Erreur dans collect_data : {str(e)}")

# Fonction 2 : Prétraitement des données
def preprocess_data(**kwargs):
    # Charger les données brutes
    df = pd.read_csv('/opt/airflow/dags/data/subventions_raw.csv')

    # Nettoyage des données
    df['secteurs_d_activites_definies_par_l_association'].fillna('Non spécifié', inplace=True)
    df['montant_vote'].fillna(0, inplace=True)
    df['nom_beneficiaire'].fillna('Inconnu', inplace=True)
    df['numero_siret'].fillna('00000000000000', inplace=True)

    # Sauvegarde des données nettoyées
    df.to_csv('/opt/airflow/dags/data/subventions_cleaned.csv', index=False)
    print("Données prétraitées et sauvegardées.")

# Fonction 3 : Stockage dans PostgreSQL
def store_to_postgresql(**kwargs):
    # Charger les données nettoyées
    df = pd.read_csv('/opt/airflow/dags/data/subventions_cleaned.csv')

    # Connexion à PostgreSQL (via le service Docker)
    conn = psycopg2.connect(
        dbname="airflow",         # Nom de la base défini dans docker-compose.yaml
        user="airflow",           # Utilisateur défini dans docker-compose.yaml
        password="airflow",       # Mot de passe défini dans docker-compose.yaml
        host="postgres",          # Nom du service Docker pour PostgreSQL
        port="5432"               # Port interne par défaut pour PostgreSQL
    )
    cursor = conn.cursor()

    # Insérer les données dans la base
    def insert_data(row):
        query = """
            INSERT INTO subventions (
                numero_de_dossier,
                annee_budgetaire,
                collectivite,
                nom_beneficiaire,
                numero_siret,
                objet_du_dossier,
                montant_vote,
                direction,
                nature_de_la_subvention,
                secteurs_d_activites_definies_par_l_association
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (
            row['numero_de_dossier'],
            row['annee_budgetaire'],
            row['collectivite'],
            row['nom_beneficiaire'],
            row['numero_siret'],
            row['objet_du_dossier'],
            row['montant_vote'],
            row['direction'],
            row['nature_de_la_subvention'],
            row['secteurs_d_activites_definies_par_l_association']
        ))

    # Appliquer les insertions ligne par ligne
    df.apply(insert_data, axis=1)

    # Commit des changements et fermeture de la connexion
    conn.commit()
    cursor.close()
    conn.close()
    print("Données insérées dans PostgreSQL (Docker) avec succès.")


# Paramètres du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialisation du DAG
with DAG(
    'subventions_pipeline',
    default_args=default_args,
    description='Pipeline complet pour les subventions parisiennes',
    schedule='@daily',
) as dag:

    # Tâche 1 : Collecte des données
    collect_task = PythonOperator(
        task_id='collect_data',
        python_callable=collect_data,
    )

    # Tâche 2 : Prétraitement des données
    preprocess_task = PythonOperator(
        task_id='preprocess_data',
        python_callable=preprocess_data,
    )

    # Tâche 3 : Stockage dans PostgreSQL
    store_task = PythonOperator(
        task_id='store_to_postgresql',
        python_callable=store_to_postgresql,
    )

    # Dépendances entre les tâches
    collect_task >> preprocess_task >> store_task
