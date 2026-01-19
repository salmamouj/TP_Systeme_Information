-- Table Patients
CREATE TABLE patients (
    id SERIAL PRIMARY KEY,
    nom VARCHAR(100) NOT NULL,
    prenom VARCHAR(100) NOT NULL,
    date_naissance DATE NOT NULL,
    adresse TEXT,
    mutuelle VARCHAR(100),
    telephone VARCHAR(20),
    email VARCHAR(150),
    date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_modification TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_patients_nom ON patients(nom);
CREATE INDEX idx_patients_date_creation ON patients(date_creation);

-- Table Services
CREATE TABLE services (
    id SERIAL PRIMARY KEY,
    nom_service VARCHAR(100) NOT NULL UNIQUE,
    capacite_lits INTEGER NOT NULL CHECK (capacite_lits > 0),
    responsable VARCHAR(100),
    etage INTEGER,
    date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table Admissions
CREATE TABLE admissions (
    id SERIAL PRIMARY KEY,
    patient_id INTEGER NOT NULL REFERENCES patients(id) ON DELETE CASCADE,
    service_id INTEGER NOT NULL REFERENCES services(id) ON DELETE RESTRICT,
    date_entree TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    date_sortie TIMESTAMP,
    score_gravite INTEGER CHECK (score_gravite BETWEEN 1 AND 10),
    motif TEXT,
    cout_total DECIMAL(10,2),
    CONSTRAINT check_dates CHECK (date_sortie IS NULL OR date_sortie > date_entree)
);

CREATE INDEX idx_admissions_patient ON admissions(patient_id);
CREATE INDEX idx_admissions_service ON admissions(service_id);
CREATE INDEX idx_admissions_dates ON admissions(date_entree, date_sortie);

-- Table Stocks Médicaments
CREATE TABLE stocks_medicaments (
    id SERIAL PRIMARY KEY,
    nom_medicament VARCHAR(200) NOT NULL UNIQUE,
    quantite INTEGER NOT NULL CHECK (quantite >= 0),
    seuil_alerte INTEGER NOT NULL CHECK (seuil_alerte >= 0),
    prix_unitaire DECIMAL(10,2) NOT NULL CHECK (prix_unitaire >= 0),
    date_peremption DATE,
    fournisseur VARCHAR(150),
    date_derniere_commande DATE,
    date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_stocks_alerte ON stocks_medicaments(quantite, seuil_alerte)
WHERE quantite <= seuil_alerte;

-- Vues pour analyses
CREATE VIEW v_alertes_stocks AS
SELECT 
    id,
    nom_medicament,
    quantite,
    seuil_alerte,
    (seuil_alerte - quantite) AS deficit,
    CASE 
        WHEN quantite = 0 THEN 'CRITIQUE'
        WHEN quantite <= seuil_alerte * 0.5 THEN 'URGENT'
        ELSE 'ATTENTION'
    END AS niveau_alerte
FROM stocks_medicaments
WHERE quantite <= seuil_alerte
ORDER BY quantite ASC;

CREATE VIEW v_occupation_services AS
SELECT 
    s.id,
    s.nom_service,
    s.capacite_lits,
    COUNT(a.id) AS lits_occupes,
    ROUND((COUNT(a.id)::NUMERIC / s.capacite_lits) * 100, 2) AS taux_occupation
FROM services s
LEFT JOIN admissions a ON s.id = a.service_id AND a.date_sortie IS NULL
GROUP BY s.id, s.nom_service, s.capacite_lits
ORDER BY taux_occupation DESC;