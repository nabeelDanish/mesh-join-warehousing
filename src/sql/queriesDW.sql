-- Query 1: Present total sales of all products supplied by each supplier 
--          with respect to quarter and month
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

-- Query 2: Present total sales of each product sold by each store. The output should be organised
--          store wise and then product wise under each store.
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

-- Query 3: Find the 5 most popular products sold over the weekends.
SELECT TR.PRODUCT_ID, P.PRODUCT_NAME, SUM(TR.QUANTITY) AS TOTAL_SOLD
FROM dw_project.transactions AS TR, dw_project.product AS P, dw_project.time AS T
WHERE TR.PRODUCT_ID = P.PRODUCT_ID 
AND TR.TIME_ID = T.TIME_ID AND T.DAY_OF_WEEK IN (1, 7)
GROUP BY TR.PRODUCT_ID, P.PRODUCT_NAME
ORDER BY SUM(TR.QUANTITY) DESC LIMIT 5;

-- Query 4: Present the quarterly sales of each product for year 2016 
--          using drill down query concept.
SELECT P.PRODUCT_ID, P.PRODUCT_NAME, 
SUM(CASE WHEN T.QUARTER = 1 THEN TR.TOTAL_SALE ELSE 0 END) AS QTR_1_SALE,
SUM(CASE WHEN T.QUARTER = 2 THEN TR.TOTAL_SALE ELSE 0 END) AS QTR_2_SALE,
SUM(CASE WHEN T.QUARTER = 3 THEN TR.TOTAL_SALE ELSE 0 END) AS QTR_3_SALE,
SUM(CASE WHEN T.QUARTER = 4 THEN TR.TOTAL_SALE ELSE 0 END) AS QTR_4_SALE
FROM dw_project.transactions AS TR, dw_project.time AS T, dw_project.product AS P
WHERE TR.PRODUCT_ID = P.PRODUCT_ID
AND TR.TIME_ID = T.TIME_ID
AND T.YEAR = 2016
GROUP BY P.PRODUCT_ID, P.PRODUCT_NAME;

-- Query 5: Extract total sales of each product for the first and 
--          second half of year 2016 along with its total yearly sales.
SELECT P.PRODUCT_ID, P.PRODUCT_NAME, 
SUM(CASE WHEN (T.QUARTER = 1 OR T.QUARTER = 2) THEN TR.TOTAL_SALE ELSE 0 END) AS FIRST_HALF_SALE,
SUM(CASE WHEN (T.QUARTER = 3 OR T.QUARTER = 4) THEN TR.TOTAL_SALE ELSE 0 END) AS SECOND_HALF_SALE,
SUM(TR.TOTAL_SALE) AS TOTAL_SALE
FROM dw_project.transactions AS TR, dw_project.time AS T, dw_project.product AS P
WHERE TR.PRODUCT_ID = P.PRODUCT_ID
AND TR.TIME_ID = T.TIME_ID
AND T.YEAR = 2016
GROUP BY P.PRODUCT_ID, P.PRODUCT_NAME;

-- Query 6: Find an anomaly in the data warehouse dataset. write a query to show the anomaly and
--          explain the anomaly in your project report.
SELECT PRODUCT_NAME, COUNT(DISTINCT PRODUCT_ID) AS PRODUCT_IDS
FROM dw_project.product
GROUP BY PRODUCT_NAME
HAVING COUNT(DISTINCT PRODUCT_ID) > 1
ORDER BY PRODUCT_NAME;

-- Query 7: Create a materialised view with name “STOREANALYSIS_MV” that presents the productwise
--          sales analysis for each store.
DROP TABLE IF EXISTS STOREANALYSIS_MV;

-- Creating the Materialised View
CREATE TABLE STOREANALYSIS_MV AS
SELECT 
    TR.STORE_ID, 
    IFNULL(TR.PRODUCT_ID, 'STORE_TOTAL') AS 'PRODUCT_ID',  
    SUM(TR.TOTAL_SALE) AS TOTAL_SALE
FROM dw_project.transactions AS TR
GROUP BY TR.STORE_ID, S.STORE_NAME, TR.PRODUCT_ID, P.PRODUCT_NAME
WITH ROLLUP
HAVING (
    TR.STORE_ID IS NOT NULL
)
ORDER BY TR.STORE_ID, TR.PRODUCT_ID;

-- Selecting from the View
SELECT * FROM STOREANALYSIS_MV;


-- Creating a trigger for inserton on the transaction fact table
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
--      BEFORE INSERTION:   540.900
--      AFTER INSERTION:    550.900
SELECT S.STORE_ID, P.PRODUCT_ID, SUM(TR.TOTAL_SALE) AS TOTAL_SALES
FROM dw_project.transactions AS TR, dw_project.store as S, dw_project.product as P
WHERE TR.STORE_ID = S.STORE_ID
AND TR.PRODUCT_ID = P.PRODUCT_ID
AND P.PRODUCT_ID = 'P-1001'
AND S.STORE_ID = 'S-1'
GROUP BY S.STORE_ID, P.PRODUCT_ID
ORDER BY S.STORE_ID, P.PRODUCT_ID;