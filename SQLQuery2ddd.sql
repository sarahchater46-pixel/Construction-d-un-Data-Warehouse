/*******************************************************************************
 PROJET DATA WAREHOUSE : ARCHITECTURE MEDALLION (BRONZE -> SILVER -> GOLD)
 Description : Ingestion, Nettoyage et Modélisation en Étoile
 *******************************************************************************/

-- =============================================
-- 0. INITIALISATION (DROP & CREATE)
-- =============================================
USE master;
GO
IF EXISTS (SELECT * FROM sys.databases WHERE name = 'DataWarehouse')
    DROP DATABASE DataWarehouse;
GO
CREATE DATABASE DataWarehouse;
GO
USE DataWarehouse;
GO

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO

-- =============================================
-- 1. COUCHE BRONZE (Tables d'Ingestion)
-- =============================================
-- On utilise VARCHAR(MAX) pour ne jamais bloquer l'importation brute.

CREATE TABLE bronze.gltransaction (
    transaction_id VARCHAR(MAX), transaction_date VARCHAR(MAX), store_code VARCHAR(MAX),
    account_number VARCHAR(MAX), amount_local VARCHAR(MAX), currency VARCHAR(MAX),
    document_number VARCHAR(MAX), description VARCHAR(MAX)
);

CREATE TABLE bronze.account (
    account_number VARCHAR(MAX), account_name VARCHAR(MAX), 
    account_type VARCHAR(MAX), currency VARCHAR(MAX)
);

CREATE TABLE bronze.storemaster (
    store_code VARCHAR(MAX), store_name VARCHAR(MAX), 
    country VARCHAR(MAX), city VARCHAR(MAX)
);

CREATE TABLE bronze.account_mapping (
    account_number VARCHAR(MAX), reporting_label VARCHAR(MAX)
);



USE DataWarehouse;
GO

-- =============================================
-- CHARGEMENT DES DONNÉES (BULK INSERT)
-- =============================================

-- 1. Chargement des Transactions
BULK INSERT bronze.gltransaction
FROM 'C:\Users\Sara Chater\Downloads\data-69f7fd84cead6276219015 (1)\gltransaction.csv' -- <--- MODIFIEZ LE CHEMIN ICI
WITH (
    FIRSTROW = 2,           -- Saute la ligne d'en-tête
    FIELDTERMINATOR = ',',  -- Séparateur de colonne (souvent ',' ou ';')
    ROWTERMINATOR = '\n',   -- Fin de ligne
    TABLOCK                 -- Optimise la performance du chargement
);

-- 2. Chargement des Comptes
BULK INSERT bronze.account
FROM 'C:\Users\Sara Chater\Downloads\data-69f7fd84cead6276219015 (1)\account.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

-- 3. Chargement des Magasins (Master Data)
BULK INSERT bronze.storemaster
FROM 'C:\Users\Sara Chater\Downloads\data-69f7fd84cead6276219015 (1)\store_master.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

-- 4. Chargement du Mapping des Comptes
BULK INSERT bronze.account_mapping
FROM 'C:\Users\Sara Chater\Downloads\data-69f7fd84cead6276219015 (1)\account_mapping.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

PRINT 'Chargement BULK INSERT terminé avec succès.';

/* NOTE : Ici, vous devez normalement exécuter vos commandes BULK INSERT.
   Exemple : 
   BULK INSERT bronze.gltransaction FROM 'C:\data\transaction.csv' 
   WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n');
*/

-- =============================================
-- 2. COUCHE SILVER (Nettoyage et Typage)
-- =============================================

-- Nettoyage des Transactions
CREATE TABLE silver.gltransaction (
    transaction_id INT PRIMARY KEY,
    transaction_date DATE,
    store_code NVARCHAR(50),
    account_number INT,
    amount_local DECIMAL(18,2),
    currency NVARCHAR(10),
    document_number NVARCHAR(100),
    description NVARCHAR(MAX)
);

INSERT INTO silver.gltransaction
SELECT 
    CAST(TRIM(transaction_id) AS INT),
    CAST(TRIM(transaction_date) AS DATE),
    UPPER(TRIM(store_code)),
    CAST(TRIM(account_number) AS INT),
    CAST(REPLACE(TRIM(amount_local), ',', '.') AS DECIMAL(18,2)),
    UPPER(TRIM(currency)),
    TRIM(document_number),
    TRIM(description)
FROM bronze.gltransaction;

-- Nettoyage des Comptes
CREATE TABLE silver.account (
    account_number INT PRIMARY KEY,
    account_name NVARCHAR(255),
    account_type NVARCHAR(100)
);

INSERT INTO silver.account
SELECT 
    CAST(TRIM(account_number) AS INT),
    TRIM(account_name),
    UPPER(TRIM(account_type))
FROM bronze.account;

-- Nettoyage des Magasins
CREATE TABLE silver.dim_store (
    store_code NVARCHAR(50) PRIMARY KEY,
    store_name NVARCHAR(255),
    country NVARCHAR(100),
    city NVARCHAR(100)
);

INSERT INTO silver.dim_store
SELECT 
    UPPER(TRIM(store_code)),
    TRIM(store_name),
    UPPER(TRIM(country)),
    UPPER(TRIM(city))
FROM bronze.storemaster;

-- Nettoyage des Magasins avec suppression des doublons
INSERT INTO silver.dim_store (store_code, store_name, country, city)
SELECT 
    UPPER(TRIM(store_code)), 
    MAX(TRIM(store_name)), -- On prend le nom le plus récent/long si doublon
    MAX(UPPER(TRIM(country))), 
    MAX(UPPER(TRIM(city)))
FROM bronze.storemaster
WHERE store_code IS NOT NULL
GROUP BY UPPER(TRIM(store_code));

-- On groupe par code pour n'avoir qu'une ligne unique
-- =============================================
-- 3. COUCHE GOLD (Modèle en Étoile - Star Schema)
-- =============================================

-- Table de Faits Finale : Enrichie pour le reporting
CREATE TABLE gold.fact_gl (
    transaction_id INT,
    transaction_date DATE,
    store_code NVARCHAR(50),
    account_number INT,
    amount_local DECIMAL(18,2),
    currency NVARCHAR(10),
    account_name NVARCHAR(255),
    account_type NVARCHAR(100),
    store_name NVARCHAR(255)
);

INSERT INTO gold.fact_gl
SELECT 
    t.transaction_id,
    t.transaction_date,
    t.store_code,
    t.account_number,
    t.amount_local,
    t.currency,
    a.account_name,
    a.account_type,
    s.store_name
FROM silver.gltransaction t
LEFT JOIN silver.account a ON t.account_number = a.account_number
LEFT JOIN silver.dim_store s ON t.store_code = s.store_code;

-- =============================================
-- 4. VUES ANALYTIQUES (Prêt pour Power BI/Tableau)
-- =============================================
GO
CREATE VIEW gold.v_pnl_summary AS
SELECT 
    account_type,
    SUM(amount_local) AS total_amount,
    currency
FROM gold.fact_gl
GROUP BY account_type, currency;
GO

PRINT 'PROJET RÉUSSI : Pipeline Bronze -> Silver -> Gold complété.';


SELECT 'Bronze Transactions' AS TableName, COUNT(*) AS Row_Count FROM bronze.gltransaction
UNION ALL
SELECT 'Silver Transactions', COUNT(*) FROM silver.gltransaction
UNION ALL
SELECT 'Gold Fact Table', COUNT(*) FROM gold.fact_gl;
SELECT 
    (SELECT COUNT(*) FROM silver.gltransaction) AS Total_Transactions,
    (SELECT COUNT(*) FROM gold.fact_gl) AS Joined_Transactions,
    ((SELECT COUNT(*) FROM silver.gltransaction) - (SELECT COUNT(*) FROM gold.fact_gl)) AS Lost_Records;

    SELECT 'Doublons Magasins' AS Check_Type, COUNT(*) FROM (
    SELECT store_code FROM silver.dim_store GROUP BY store_code HAVING COUNT(*) > 1
) AS s
UNION ALL
SELECT 'Montants Nuls', COUNT(*) FROM silver.gltransaction WHERE amount_local IS NULL;

-- Top 5 des dépenses par type de compte
SELECT TOP 5 
    account_type, 
    account_name, 
    SUM(amount_local) AS Total_Amount
FROM gold.fact_gl
GROUP BY account_type, account_name
ORDER BY Total_Amount ASC; -- ASC car les dépenses sont souvent négatives
SELECT '1. BRONZE (Brut)' AS Etape, COUNT(*) AS Nb_Lignes FROM bronze.gltransaction
UNION ALL
SELECT '2. SILVER (Nettoyé)', COUNT(*) FROM silver.gltransaction
UNION ALL
SELECT '3. GOLD (Analytique)', COUNT(*) FROM gold.fact_gl;

SELECT * FROM gold.fact_gl;

SELECT * FROM gold.fact_gl 
ORDER BY transaction_date DESC;

SELECT 
    f.transaction_id AS [ID],
    f.transaction_date AS [Date],
    f.document_number AS [Document],
    f.store_name AS [Magasin],
    f.account_name AS [Compte],
    f.account_type AS [Catégorie],
    f.amount_local AS [Montant],
    f.currency AS [Devise],
    f.description AS [Description]
FROM gold.fact_gl f
ORDER BY f.transaction_date DESC;




SELECT 
    (SELECT COUNT(*) FROM bronze.gltransaction) AS Nb_Bronze,
    (SELECT COUNT(*) FROM silver.gltransaction) AS Nb_Silver,
    CASE 
        WHEN (SELECT COUNT(*) FROM bronze.gltransaction) = (SELECT COUNT(*) FROM silver.gltransaction) 
        THEN '✅ SUCCÈS : Aucune perte' 
        ELSE '⚠️ ALERTE : Différence détectée' 
    END AS Statut;



    SELECT 'Transactions sans magasin' AS Alerte, COUNT(*) AS Total
FROM silver.gltransaction t
LEFT JOIN silver.dim_store s ON t.store_code = s.store_code
WHERE s.store_code IS NULL;

SELECT 'Transactions sans compte' AS Alerte, COUNT(*) AS Total
FROM silver.gltransaction t
LEFT JOIN silver.account a ON t.account_number = a.account_number
WHERE a.account_number IS NULL;

SELECT transaction_id, COUNT(*) as Frequence
FROM silver.gltransaction
GROUP BY transaction_id
HAVING COUNT(*) > 1;



SELECT 
    'BRONZE' as Couche, SUM(CAST(REPLACE(amount_local, ',', '.') AS DECIMAL(18,2))) as Total_Somme
FROM bronze.gltransaction
UNION ALL
SELECT 
    'GOLD', SUM(amount_local)
FROM gold.fact_gl;