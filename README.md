📋 Description du Projet
Ce projet consiste en la mise en place d'un Data Warehouse complet pour la chaîne de magasins NRG Retail, opérant en Suède et en Norvège. L'objectif est de transformer des données financières brutes (CSV) en un modèle analytique performant (Star Schema) pour piloter la rentabilité et l'efficacité marketing via Power BI.

🏗️ Architecture des Données (Modèle Medallion)
Le projet suit l'architecture Medallion pour garantir la qualité et la traçabilité de la donnée :

🥉 Couche Bronze (Raw)
Source : Fichiers CSV (transactions, stores, accounts).

Action : Ingestion brute via BULK INSERT dans SQL Server.

Objectif : Conservation de l'historique immuable "tel quel".

🥈 Couche Silver (Cleaned)
Nettoyage : Suppression des espaces (TRIM), mise en majuscules (UPPER).

Typage : Conversion des colonnes texte en DECIMAL et DATE.

Qualité : Déduplication des magasins (gestion de l'anomalie Magasin 5100) et validation des intégrités référentielles.

🥇 Couche Gold (Analytical)
Modélisation : Mise en place d'un Modèle en Étoile (Star Schema).

Tables :

fact_gl : Table de faits contenant toutes les transactions financières.

dimstore : Dimension des points de vente (Suède vs Norvège).

dimaccount : Dimension du plan comptable (Revenus, COGS, OPEX).

🛠️ Technologies Utilisées
Base de données : SQL Server (T-SQL)

ETL : Scripts SQL de transformation (Architecture Medallion)

BI & Visualisation : Power BI Desktop

Langage d'analyse : DAX (Data Analysis Expressions)

📈 Indicateurs Clés de Performance (KPIs)
Le projet permet de suivre en temps réel les métriques suivantes :

Total Revenue : Somme des revenus par canal et par pays.

Gross Margin : Analyse de la rentabilité après coûts directs.

Marketing ROI : Score actuel de 1.3 (Analyse de l'efficacité publicitaire).

OPEX Analysis : Suivi granulaire des dépenses opérationnelles par magasin.

