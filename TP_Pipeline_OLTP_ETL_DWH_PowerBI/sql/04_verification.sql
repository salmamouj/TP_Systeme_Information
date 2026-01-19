USE ventes_dwh;

SELECT COUNT(*) FROM DimClient;
SELECT COUNT(*) FROM DimProduit;
SELECT * FROM DimDate ORDER BY date_complete LIMIT 10;
SELECT COUNT(*) FROM FactVentes;