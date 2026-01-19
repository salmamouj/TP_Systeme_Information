from faker import Faker
import psycopg2
from datetime import datetime, timedelta
import random

# Configuration
fake = Faker('fr_FR')
DB_CONFIG = {
    'host': 'localhost',
    'database': 'healthflow_source',
    'user': 'healthflow_user',
    'password': 'HealthFlow2025!'
}

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def generate_services(conn, nb_services=8):
    """Génère les services hospitaliers"""
    services = [
        ('Urgences', 40, 'Dr. Martin', 1),
        ('Cardiologie', 30, 'Dr. Dubois', 2),
        ('Pédiatrie', 25, 'Dr. Bernard', 3),
        ('Chirurgie', 35, 'Dr. Petit', 2),
        ('Maternité', 20, 'Dr. Durand', 4),
        ('Gériatrie', 28, 'Dr. Moreau', 3),
        ('Pneumologie', 22, 'Dr. Laurent', 1),
        ('Neurologie', 18, 'Dr. Simon', 2)
    ]
    
    cursor = conn.cursor()
    for nom, capacite, responsable, etage in services[:nb_services]:
        cursor.execute("""
            INSERT INTO services (nom_service, capacite_lits, responsable, etage)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (nom_service) DO NOTHING
        """, (nom, capacite, responsable, etage))
    
    conn.commit()
    cursor.close()
    print(f"✅ {nb_services} services créés")

def generate_patients(conn, nb_patients=1000):
    """Génère les patients"""
    cursor = conn.cursor()
    mutuelles = ['MGEN', 'Harmonie Mutuelle', 'MAIF', 'Groupama', 'AXA', 'Malakoff', None]
    
    for i in range(nb_patients):
        nom = fake.last_name()
        prenom = fake.first_name()
        date_naissance = fake.date_of_birth(minimum_age=0, maximum_age=95)
        adresse = fake.address().replace('\n', ', ')
        mutuelle = random.choice(mutuelles)
        telephone = fake.phone_number()
        email = f"{prenom.lower()}.{nom.lower()}@{fake.free_email_domain()}"
        
        cursor.execute("""
            INSERT INTO patients 
            (nom, prenom, date_naissance, adresse, mutuelle, telephone, email)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (nom, prenom, date_naissance, adresse, mutuelle, telephone, email))
        
        if (i + 1) % 100 == 0:
            print(f"   Patients créés: {i + 1}/{nb_patients}")
    
    conn.commit()
    cursor.close()
    print(f"✅ {nb_patients} patients créés")

def generate_admissions(conn, nb_admissions=2000):
    """Génère les hospitalisations"""
    cursor = conn.cursor()
    
    cursor.execute("SELECT id FROM patients")
    patient_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT id FROM services")
    service_ids = [row[0] for row in cursor.fetchall()]
    
    motifs = [
        'Accident de voiture', 'Crise cardiaque', 'Pneumonie',
        'Fracture', 'Accouchement', 'Opération programmée',
        'Insuffisance respiratoire', 'AVC', 'Diabète décompensé'
    ]
    
    for i in range(nb_admissions):
        patient_id = random.choice(patient_ids)
        service_id = random.choice(service_ids)
        
        date_entree = fake.date_time_between(start_date='-6M', end_date='now')
        
        if random.random() < 0.7:
            duree_sejour = random.randint(1, 30)
            date_sortie = date_entree + timedelta(days=duree_sejour)
        else:
            date_sortie = None
        
        score_gravite = random.randint(1, 10)
        motif = random.choice(motifs)
        cout_total = round(random.uniform(500, 15000), 2) if date_sortie else None
        
        cursor.execute("""
            INSERT INTO admissions 
            (patient_id, service_id, date_entree, date_sortie, score_gravite, motif, cout_total)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (patient_id, service_id, date_entree, date_sortie, score_gravite, motif, cout_total))
        
        if (i + 1) % 200 == 0:
            print(f"   Admissions créées: {i + 1}/{nb_admissions}")
    
    conn.commit()
    cursor.close()
    print(f"✅ {nb_admissions} admissions créées")

def generate_stocks(conn, nb_medicaments=150):
    """Génère le stock de médicaments"""
    cursor = conn.cursor()
    
    medicaments_base = [
        'Paracétamol', 'Ibuprofène', 'Amoxicilline', 'Doliprane',
        'Aspégic', 'Ventoline', 'Insuline', 'Morphine',
        'Tramadol', 'Cortisone', 'Antibiotique', 'Antidouleur'
    ]
    
    fournisseurs = ['Sanofi', 'Pfizer', 'Roche', 'Novartis', 'GSK']
    
    for i in range(nb_medicaments):
        nom = f"{random.choice(medicaments_base)} {fake.bothify('###??').upper()}"
        quantite = random.randint(0, 1000)
        seuil_alerte = random.randint(50, 200)
        prix = round(random.uniform(5, 500), 2)
        date_peremption = fake.date_between(start_date='today', end_date='+2y')
        fournisseur = random.choice(fournisseurs)
        date_commande = fake.date_between(start_date='-3M', end_date='today')
        
        cursor.execute("""
            INSERT INTO stocks_medicaments 
            (nom_medicament, quantite, seuil_alerte, prix_unitaire, 
             date_peremption, fournisseur, date_derniere_commande)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (nom_medicament) DO NOTHING
        """, (nom, quantite, seuil_alerte, prix, date_peremption, fournisseur, date_commande))
    
    conn.commit()
    cursor.close()
    print(f"✅ {nb_medicaments} médicaments en stock")

if _name_ == "_main_":
    print("🚀 Démarrage de la génération de données HealthFlow...")
    print("=" * 60)
    
    conn = connect_db()
    
    try:
        print("\n📦 Génération des services...")
        generate_services(conn, nb_services=8)
        
        print("\n👥 Génération des patients...")
        generate_patients(conn, nb_patients=1000)
        
        print("\n🏥 Génération des admissions...")
        generate_admissions(conn, nb_admissions=2000)
        
        print("\n💊 Génération des stocks médicaments...")
        generate_stocks(conn, nb_medicaments=150)
        
        print("\n" + "=" * 60)
        print("✅ GÉNÉRATION TERMINÉE AVEC SUCCÈS!")
        print("=" * 60)
        
        # Statistiques
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM patients")
        print(f"📊 Total Patients: {cursor.fetchone()[0]}")
        
        cursor.execute("SELECT COUNT(*) FROM admissions")
        print(f"📊 Total Admissions: {cursor.fetchone()[0]}")
        
        cursor.execute("SELECT COUNT(*) FROM admissions WHERE date_sortie IS NULL")
        print(f"📊 Patients actuellement hospitalisés: {cursor.fetchone()[0]}")
        
        cursor.execute("SELECT COUNT(*) FROM stocks_medicaments WHERE quantite <= seuil_alerte")
        print(f"⚠️  Alertes stock critiques: {cursor.fetchone()[0]}")
        
        cursor.close()
        
    except Exception as e:
        print(f"❌ ERREUR: {e}")
        conn.rollback()
    
    finally:
        conn.close()
        print("\n🔌 Connexion fermée.")