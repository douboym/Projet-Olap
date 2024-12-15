from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import IntegerType, FloatType
import psycopg2


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
        tables_to_check = ['subventions_raw', 'subventions_cleaned']
        for table in tables_to_check:
            check_table_query = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = '{table}'
            );
            """
            cursor.execute(check_table_query)
            table_exists = cursor.fetchone()[0]

            
            # If the table doesn't exist, create it
            if not table_exists:
                create_table_query = f"""
                CREATE TABLE {table} (
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
                print(f"Table {table} has been created successfully.")
            else:
                print(f"Table {table} already exists.")

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
            
            spark = SparkSession.builder \
                .appName("Collect and Insert raw data into PostgreSQL") \
                .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.7.4.jar") \
                .config("spark.executor.extraClassPath", "/opt/spark/jars/postgresql-42.7.4.jar") \
                .getOrCreate()
                
            df = spark.createDataFrame(all_records)
            df = df.withColumn("annee_budgetaire", df["annee_budgetaire"].cast(IntegerType())) \
                .withColumn("montant_vote", df["montant_vote"].cast(FloatType()))
            
            df.write \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://postgres:5432/airflow") \
                .option("dbtable", 'subventions_raw') \
                .option("user", "airflow") \
                .option("password", "airflow") \
                .mode("overwrite") \
                .save()
            
            print("Données enregistrées avec succès dans Postgres.")
            
            spark.stop()
        else:
            print("Aucune donnée collectée.")

    except Exception as e:
        print(f"Erreur dans collect_data : {str(e)}")

# Fonction 2 : Prétraitement des données
def preprocess_data(**kwargs):
    try:
        spark = SparkSession.builder \
            .appName("Collect and Insert raw data into PostgreSQL") \
            .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.7.4.jar") \
            .config("spark.executor.extraClassPath", "/opt/spark/jars/postgresql-42.7.4.jar") \
            .getOrCreate()
            
        properties = {
            "user": "airflow",
            "password": "airflow",
            "driver": "org.postgresql.Driver"
        }

        # Read data from PostgreSQL into a Spark DataFrame
        df = spark.read.jdbc(url="jdbc:postgresql://postgres:5432/airflow", table="subventions_raw", properties=properties)
        print("Données récupérées de PostgreSQL.")
        
        # Preprocessing and cleaning the data
        df = df.withColumn(
            "secteurs_d_activites_definies_par_l_association", 
            when(col("secteurs_d_activites_definies_par_l_association").isNull(), "Non spécifié")
            .otherwise(col("secteurs_d_activites_definies_par_l_association").cast("string"))
        )
        
        df = df.withColumn(
            "montant_vote", 
            when(col("montant_vote").isNull(), 0).otherwise(col("montant_vote")).cast("float")
        )
        
        df = df.withColumn(
            "nom_beneficiaire", 
            when(col("nom_beneficiaire").isNull(), "Inconnu").otherwise(col("nom_beneficiaire").cast("string"))
        )
        
        df = df.withColumn(
            "numero_siret", 
            when(col("numero_siret").isNull(), "00000000000000").otherwise(col("numero_siret").cast("string"))
        )

        df = df.withColumn(
            "annee_budgetaire", 
            when(col("annee_budgetaire").isNull(), None).otherwise(col("annee_budgetaire")).cast("integer")
        )
        
        # Filtrer les valeurs hors limite
        df = df.filter(col("montant_vote") <= 9223372036854775807)  # BIGINT max value
        df = df.filter(col("annee_budgetaire") <= 2147483647)  # INTEGER max value
        
        df_cleaned = df.dropna()

        df_cleaned.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/airflow") \
            .option("dbtable", 'subventions_cleaned') \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .mode("overwrite") \
            .save()
        
        print("Données prétraitées et sauvegardées.")
        
        spark.stop()

    except Exception as e:
        print(f"Erreur dans preprocess_data : {str(e)}")

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
    
    check_table_task >> collect_task >> preprocess_task
    