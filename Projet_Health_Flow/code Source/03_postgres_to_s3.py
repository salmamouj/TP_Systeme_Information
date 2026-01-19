import psycopg2
import pandas as pd
import boto3
from datetime import datetime
import io
import os

# ⚠️ IMPORTANT: Remplacer par vos vraies valeurs
DB_CONFIG = {
    'host': 'localhost',
    'database': 'healthflow_source',
    'user': 'healthflow_user',
    'password': 'HealthFlow2025!'
}

AWS_CONFIG = {
    'bucket_name': 'healthflow-lake-2025',
    'aws_access_key_id': 'VOTRE_ACCESS_KEY_ICI',
    'aws_secret_access_key': 'VOTRE_SECRET_KEY_ICI',
    'region_name': 'eu-west-3'
}

def extract_table_to_s3(table_name, db_config, aws_config):
    """
    Extrait une table PostgreSQL et l'exporte vers S3
    """
    print(f"\n📤 Extraction de la table: {table_name}")
    
    # Connexion PostgreSQL
    conn = psycopg2.connect(**db_config)
    
    # Extraction des données
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, conn)
    conn.close()
    
    print(f"   ✓ {len(df)} lignes extraites")
    
    # Ajout des métadonnées d'audit
    df['_extracted_at'] = datetime.now()
    df['_source_system'] = 'PostgreSQL_OLTP'
    df['extraction_batch_id'] = datetime.now().strftime('%Y%m%d%H%M%S')
    
    # Conversion en CSV
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, encoding='utf-8')
    
    # Chemin S3 avec partition temporelle
    current_date = datetime.now()
    s3_key = (f"bronze/{table_name}/"
              f"year={current_date.year}/"
              f"month={current_date.month:02d}/"
              f"day={current_date.day:02d}/"
              f"{table_name}{current_date.strftime('%Y%m%d%H%M%S')}.csv")
    
    # Upload vers S3
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_config['aws_access_key_id'],
        aws_secret_access_key=aws_config['aws_secret_access_key'],
        region_name=aws_config['region_name']
    )
    
    s3_client.put_object(
        Bucket=aws_config['bucket_name'],
        Key=s3_key,
        Body=csv_buffer.getvalue(),
        ContentType='text/csv'
    )
    
    s3_path = f"s3://{aws_config['bucket_name']}/{s3_key}"
    print(f"   ✓ Uploadé vers: {s3_path}")
    
    return {
        'table': table_name,
        'rows': len(df),
        's3_path': s3_path,
        'timestamp': datetime.now()
    }

def main():
    """Export toutes les tables vers S3"""
    
    print("=" * 70)
    print("🚀 HEALTHFLOW - EXTRACTION POSTGRESQL → AWS S3 (BRONZE LAYER)")
    print("=" * 70)
    print(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🪣 Bucket: {AWS_CONFIG['bucket_name']}")
    
    tables = ['patients', 'services', 'admissions', 'stocks_medicaments']
    
    results = []
    
    for table in tables:
        try:
            result = extract_table_to_s3(table, DB_CONFIG, AWS_CONFIG)
            results.append(result)
        except Exception as e:
            print(f"   ❌ Erreur pour {table}: {e}")
    
    # Résumé
    print("\n" + "=" * 70)
    print("📊 RÉSUMÉ DE L'EXTRACTION")
    print("=" * 70)
    
    total_rows = sum(r['rows'] for r in results)
    print(f"✅ Total de lignes extraites: {total_rows:,}")
    
    for result in results:
        print(f"   • {result['table']}: {result['rows']:,} lignes")
    
    print("\n✅ EXTRACTION TERMINÉE AVEC SUCCÈS!")
    print("=" * 70)

if _name_ == "_main_":
    main()