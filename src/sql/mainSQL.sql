-- Queries for Given Database Manipulation
SELECT * FROM dwhproject.transactions LIMIT 10;
SELECT * FROM dwhproject.masterdata;
SELECT COUNT(*) FROM dwhproject.transactions;
SELECT COUNT(*) FROM dwhproject.masterdata;
SELECT * FROM dwhproject.transactions WHERE STORE_NAME IS NULL;
SELECT * FROM dwhproject.transactions WHERE PRODUCT_ID NOT IN (SELECT PRODUCT_ID FROM dwhproject.masterdata);

-- Queries for Warehouse Selection
SELECT * FROM dw_project.customer;
SELECT * FROM dw_project.product;
SELECT * FROM dw_project.time;
SELECT * FROM dw_project.store;
SELECT * FROM dw_project.supplier;
SELECT * FROM dw_project.transactions;

-- Queries for Warehouse Selection and Manipulation
SELECT MAX(TIME_ID) FROM dw_project.time;
SELECT COUNT(*) FROM dw_project.transactions;
SELECT * FROM dw_project.transactions LIMIT 10 OFFSET 0;

-- Queries for Dropping Tables from Warehouse
DROP TABLE dw_project.customer;
DROP TABLE dw_project.product;
DROP TABLE dw_project.store;
DROP TABLE dw_project.supplier;
DROP TABLE dw_project.time;
DROP TABLE dw_project.transactions;

-- Queries for Deleting Records
DELETE FROM dw_project.time WHERE TIME_ID > 0;
DELETE FROM dw_project.transactions WHERE TOTAL_SALE > 0;

-- OLAP Query #1: 
SELECT 
	TR.SUPPLIER_ID, 
    S.SUPPLIER_NAME, 
    T.`YEAR`, 
    T.`QUARTER`,
    IFNULL(T.`MONTH`, 'Quarter_Total') AS 'MONTH', 
    SUM(TR.TOTAL_SALE) AS TOTAL_SALE
FROM dw_project.supplier AS S, dw_project.transactions AS TR, dw_project.time AS T
WHERE TR.SUPPLIER_ID = S.SUPPLIER_ID
AND T.`YEAR` = 2016
AND TR.TIME_ID = T.TIME_ID
GROUP BY TR.SUPPLIER_ID, S.SUPPLIER_NAME, T.`QUARTER`, T.`MONTH` 
WITH ROLLUP
HAVING (TR.SUPPLIER_ID IS NOT NULL AND S.SUPPLIER_NAME IS NOT NULL AND T.`QUARTER` IS NOT NULL)
ORDER BY TR.SUPPLIER_ID;

-- OLAP Query #2
SELECT 
	TR.STORE_ID, 
    S.STORE_NAME, 
    IFNULL(TR.PRODUCT_ID, 'STORE_TOTAL') AS 'PRODUCT_ID', 
    IFNULL(P.PRODUCT_NAME, 'STORE_TOTAL') AS 'PRODUCT_NAME', 
    SUM(TR.TOTAL_SALE) AS TOTAL_SALE
FROM dw_project.transactions AS TR, dw_project.store AS S, dw_project.product AS P
WHERE TR.STORE_ID = S.STORE_ID
AND TR.PRODUCT_ID = P.PRODUCT_ID
GROUP BY TR.STORE_ID, S.STORE_NAME, TR.PRODUCT_ID, P.PRODUCT_NAME
WITH ROLLUP
HAVING (
	TR.STORE_ID IS NOT NULL AND 
    S.STORE_NAME IS NOT NULL AND (
		(TR.PRODUCT_ID IS NULL AND P.PRODUCT_NAME IS NULL) OR
        (TR.PRODUCT_ID IS NOT NULL AND P.PRODUCT_NAME IS NOT NULL)
    )
)
ORDER BY TR.STORE_ID, TR.PRODUCT_ID;

-- OLAP Query #3
SELECT TR.PRODUCT_ID, P.PRODUCT_NAME, SUM(TR.QUANTITY) AS TOTAL_SOLD
FROM dw_project.transactions AS TR, dw_project.product AS P, dw_project.time AS T
WHERE TR.PRODUCT_ID = P.PRODUCT_ID 
AND TR.TIME_ID = T.TIME_ID AND T.DAY_OF_WEEK IN (1, 7)
GROUP BY TR.PRODUCT_ID, P.PRODUCT_NAME
ORDER BY SUM(TR.QUANTITY) DESC LIMIT 5;

SELECT P.PRODUCT_ID, P.PRODUCT_NAME, 
SUM(CASE WHEN (T.QUARTER = 1 OR T.QUARTER = 2) THEN TR.TOTAL_SALE ELSE 0 END) AS FIRST_HALF_SALE,
SUM(CASE WHEN (T.QUARTER = 3 OR T.QUARTER = 4) THEN TR.TOTAL_SALE ELSE 0 END) AS SECOND_HALF_SALE,
SUM(TR.TOTAL_SALE) AS TOTAL_SALE
FROM dw_project.transactions AS TR, dw_project.time AS T, dw_project.product AS P
WHERE TR.PRODUCT_ID = P.PRODUCT_ID
AND TR.TIME_ID = T.TIME_ID
AND T.YEAR = 2016
GROUP BY P.PRODUCT_ID, P.PRODUCT_NAME;

SELECT P.PRODUCT_ID, P.PRODUCT_NAME, 
SUM(CASE WHEN T.QUARTER = 1 THEN TR.TOTAL_SALE ELSE 0 END) AS QTR_1_SALE,
SUM(CASE WHEN T.QUARTER = 2 THEN TR.TOTAL_SALE ELSE 0 END) AS QTR_2_SALE,
SUM(CASE WHEN T.QUARTER = 3 THEN TR.TOTAL_SALE ELSE 0 END) AS QTR_3_SALE,
SUM(CASE WHEN T.QUARTER = 4 THEN TR.TOTAL_SALE ELSE 0 END) AS QTR_4_SALE
FROM dw_project.transactions AS TR, dw_project.time AS T, dw_project.product AS P
WHERE TR.PRODUCT_ID = P.PRODUCT_ID
AND TR.TIME_ID = T.TIME_ID
GROUP BY P.PRODUCT_ID, P.PRODUCT_NAME;

SELECT PRODUCT_NAME, COUNT(DISTINCT PRODUCT_ID) AS PRODUCT_IDS
FROM dw_project.product
GROUP BY PRODUCT_NAME
HAVING COUNT(DISTINCT PRODUCT_ID) > 1
ORDER BY PRODUCT_NAME;

DROP TABLE IF EXISTS dw_project.STOREANALYSIS_MV;
CREATE TABLE dw_project.STOREANALYSIS_MV AS
SELECT 
    TR.STORE_ID, 
    IFNULL(TR.PRODUCT_ID, 'STORE_TOTAL') AS 'PRODUCT_ID',  
    SUM(TR.TOTAL_SALE) AS TOTAL_SALE
FROM dw_project.transactions AS TR
GROUP BY TR.STORE_ID, TR.PRODUCT_ID
WITH ROLLUP
HAVING (
    TR.STORE_ID IS NOT NULL
)
ORDER BY TR.STORE_ID, TR.PRODUCT_ID;

SELECT COUNT(*) FROM dw_project.STOREANALYSIS_MV;

DROP TRIGGER IF EXISTS dw_project.INSERT_TRANSACTION;

DELIMITER // 
CREATE TRIGGER dw_project.INSERT_TRANSACTION
AFTER INSERT ON dw_project.transactions
FOR EACH ROW
BEGIN
	DECLARE SALE_TOTAL DECIMAL(7, 2);
    DECLARE REQUIRE_UPDATE NUMERIC(1);
    
    SET REQUIRE_UPDATE := 0;
    SELECT 1, TOTAL_SALE INTO REQUIRE_UPDATE, SALE_TOTAL
    FROM dw_project.STOREANALYSIS_MV
    WHERE STORE_ID = NEW.STORE_ID AND PRODUCT_ID = NEW.PRODUCT_ID;
    
    IF REQUIRE_UPDATE = 0 THEN
		INSERT INTO dw_project.STOREANALYSIS_MV VALUES(NEW.STORE_ID, NEW.PRODUCT_ID, NEW.TOTAL_SALE);
    ELSE
		SET SALE_TOTAL := SALE_TOTAL + NEW.TOTAL_SALE;
        UPDATE dw_project.STOREANALYSIS_MV
        SET TOTAL_SALE = SALE_TOTAL
        WHERE STORE_ID = NEW.STORE_ID AND PRODUCT_ID = NEW.PRODUCT_ID;
    END IF;
END;
//
DELIMITER ;

-- Query for insertion
INSERT INTO dw_project.transactions(PRODUCT_ID, SUPPLIER_ID, STORE_ID, CUSTOMER_ID, TIME_ID, QUANTITY, TOTAL_SALE) 
VALUES('P-1001', 'SP-1', 'S-1', 'C-15', '2016-08-22', 1, 10);

-- Query to Test the Trigger
SELECT STORE_ID, PRODUCT_ID, TOTAL_SALE
FROM dw_project.storeanalysis_mv
WHERE PRODUCT_ID = 'P-1001'
AND STORE_ID = 'S-1';

-- This query is used to test the triggers on STOREANALYIS_MV table
-- This Query should give the following results:
-- 		BEFORE INSERTION: 	540.900
-- 		AFTER INSERTION: 	550.900
SELECT S.STORE_ID, P.PRODUCT_ID, SUM(TR.TOTAL_SALE) AS TOTAL_SALES
FROM dw_project.transactions AS TR, dw_project.store as S, dw_project.product as P
WHERE TR.STORE_ID = S.STORE_ID
AND TR.PRODUCT_ID = P.PRODUCT_ID
AND P.PRODUCT_ID = 'P-1001'
AND S.STORE_ID = 'S-1'
GROUP BY S.STORE_ID, P.PRODUCT_ID
ORDER BY S.STORE_ID, P.PRODUCT_ID;