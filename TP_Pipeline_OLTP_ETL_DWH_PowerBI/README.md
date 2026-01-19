Titre: "TP : Pipeline OLTP → ETL Pentaho PDI → Data Warehouse → Reporting Power BI"

Objectif:
        -Mettre en place une chaîne complète de Business Intelligence (de l’extraction des données transactionnelles jusqu’à la visualisation interactive) pour transformer des données brutes de ventes en insights décisionnels exploitables par la direction de TechStore.
        -Apprendre à séparer OLTP (opérationnel) et OLAP (analytique), construire un Data Warehouse simple, automatiser un processus ETL avec un outil graphique (Pentaho PDI), et créer un dashboard Power BI pour répondre à des questions business concrètes.

Technologies utilisées:
        -MySQL Workbench
        -Pentaho
        -Power BI
        -python
        -faker, pandas

1- creation des base de donnees:
        -01_create_oltp.sql
        -02_create_dwh.sql
2- generation des donnees "python generate_data.py"
3- execution des transformations:
        -dim_client.ktr
        -dim_produit.ktr
        -dim_date.ktr
        -fact_ventes.ktr
4- execution du job:
        -job_etl_complet.kjb
6- verification de "ventes_dwh":
        -03_queries_olap
        -04_verification
5- connexion avec Power Bi
