from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import psycopg2
import os


def check_and_create_table():
    """
    Check if the 'subventions' table exists in PostgreSQL, and create it if it doesn't.
    """
    try:
        conn = psycopg2.connect(
            dbname="airflow",
            user="airflow",
            password="airflow",
            host="postgres",
            port="5432"
        )
        cursor = conn.cursor()

        # SQL to check if the table exists
        check_table_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'subventions'
        );
        """
        cursor.execute(check_table_query)
        table_exists = cursor.fetchone()[0]

        # If the table doesn't exist, create it
        if not table_exists:
            create_table_query = """
            CREATE TABLE subventions (
                numero_de_dossier VARCHAR PRIMARY KEY,
                annee_budgetaire INTEGER,
                collectivite VARCHAR,
                nom_beneficiaire VARCHAR,
                numero_siret VARCHAR,
                objet_du_dossier TEXT,
                montant_vote FLOAT,
                direction VARCHAR,
                nature_de_la_subvention VARCHAR,
                secteurs_d_activites_definies_par_l_association VARCHAR
            );
            """
            cursor.execute(create_table_query)
            conn.commit()
            print("Table 'subventions' has been created successfully.")
        else:
            print("Table 'subventions' already exists.")

        # Close the connection
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error in check_and_create_table: {e}")

# Fonction 1 : Collecte des données
def collect_data():
    try:
        print("Début de la collecte des données...")
        base_url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/subventions-associations-votees-/records"
        all_records = []
        limit = 100  # Nombre maximum de résultats par requête
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

        # Vérifier si des données ont été récupérées
        if all_records:
            print(f"Nombre total de records collectés : {len(all_records)}")
            df = pd.DataFrame(all_records)
            
            # Créer un fichier temporaire pour éviter l'accumulation
            temp_file = '/opt/airflow/dags/data/subventions_raw.csv'
            if os.path.exists(temp_file):
                os.remove(temp_file)
            df.to_csv(temp_file, index=False)
            print("Données enregistrées avec succès dans le fichier temporaire.")
        else:
            print("Aucune donnée collectée.")

    except Exception as e:
        print(f"Erreur dans collect_data : {str(e)}")

# Fonction 2 : Prétraitement des données
def preprocess_data(**kwargs):
    try:
        temp_file = '/opt/airflow/dags/data/subventions_raw.csv'
        if not os.path.exists(temp_file):
            print(f"Fichier {temp_file} introuvable. Annulation du prétraitement.")
            return

        df = pd.read_csv(temp_file)

        # Nettoyage et conversions
        df['secteurs_d_activites_definies_par_l_association'] = df['secteurs_d_activites_definies_par_l_association'].fillna('Non spécifié')
        df['montant_vote'] = pd.to_numeric(df['montant_vote'], errors='coerce').fillna(0).astype('float64')
        df['nom_beneficiaire'] = df['nom_beneficiaire'].fillna('Inconnu')
        df['numero_siret'] = df['numero_siret'].fillna('00000000000000')
        df['annee_budgetaire'] = pd.to_numeric(df['annee_budgetaire'], errors='coerce')

        # Filtrer les valeurs hors limite
        df = df[df['montant_vote'] <= 9223372036854775807]  # BIGINT max
        df = df[df['annee_budgetaire'] <= 2147483647]  # INTEGER max

        # Sauvegarde dans un fichier nettoyé
        cleaned_file = '/opt/airflow/dags/data/subventions_cleaned.csv'
        if os.path.exists(cleaned_file):
            os.remove(cleaned_file)
        df.to_csv(cleaned_file, index=False)
        print("Données prétraitées et sauvegardées.")

    except Exception as e:
        print(f"Erreur dans preprocess_data : {str(e)}")

def store_to_postgresql(**kwargs):
    # Charger les données nettoyées
    df = pd.read_csv('/opt/airflow/dags/data/subventions_cleaned.csv')

    # Connexion à PostgreSQL
    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="postgres",
        port="5432"
    )
    cursor = conn.cursor()

    # Fonction pour insérer une ligne
    def insert_data(row):
        try:
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
                int(row['annee_budgetaire']) if pd.notna(row['annee_budgetaire']) else None,
                row['collectivite'],
                row['nom_beneficiaire'],
                row['numero_siret'],
                row['objet_du_dossier'],
                float(row['montant_vote']) if pd.notna(row['montant_vote']) else None,
                row['direction'],
                row['nature_de_la_subvention'],
                row['secteurs_d_activites_definies_par_l_association']
            ))
            conn.commit()  # Commit uniquement si l'insertion réussit
        except Exception as e:
            conn.rollback()  # Rétablir la transaction si une erreur survient
            print(f"Erreur lors de l'insertion de la ligne : {row}")
            print(f"Exception : {e}")

    # Appliquer les insertions ligne par ligne
    df.apply(insert_data, axis=1)

    # Fermeture de la connexion
    cursor.close()
    conn.close()
    print("Données insérées dans PostgreSQL avec succès.")


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

with DAG(
    'subventions_pipeline',
    default_args=default_args,
    description='Pipeline complet pour les subventions parisiennes',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    check_table_task = PythonOperator(
        task_id='check_and_create_table',
        python_callable=check_and_create_table,
    )

    collect_task = PythonOperator(
        task_id='collect_data',
        python_callable=collect_data,
    )

    preprocess_task = PythonOperator(
        task_id='preprocess_data',
        python_callable=preprocess_data,
    )

    store_task = PythonOperator(
        task_id='store_to_postgresql',
        python_callable=store_to_postgresql,
    )

    check_table_task >> collect_task >> preprocess_task >> store_task
