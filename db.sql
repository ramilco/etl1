CREATE SCHEMA raw;

CREATE SEQUENCE serial;

CREATE TABLE RAW.DATAPOINTS(
D_ID				VARCHAR,
CREATED				VARCHAR,
ORIGIN_PID			VARCHAR,
DESTINATION_PID		VARCHAR,
VALID_FROM			VARCHAR,
VALID_TO			VARCHAR,
COMPANY_ID			VARCHAR,
SUPPLIER_ID			VARCHAR,
EQUIPMENT_ID 		VARCHAR,
SRC_FILE 	 		VARCHAR, 	-- FileName
RAW_LOAD_TIME		TIMESTAMP, 	-- loaded to RAW
STG_STATUS			VARCHAR, 	-- 'NEW', 'LOADED', 'ERROR'
STG_LOAD_TIME		TIMESTAMP, 	-- tried to load to STG
RAW_PID				BIGINT
);

CREATE TABLE RAW.CHARGES(
D_ID 				VARCHAR,	
CURRENCY 			VARCHAR,	
CHARGE_VALUE 		VARCHAR,
SRC_FILE 	 		VARCHAR, 	-- FileName
RAW_LOAD_TIME		TIMESTAMP, 	-- loaded to RAW
STG_STATUS			VARCHAR, 	-- 'NEW', 'LOADED', 'ERROR'
STG_LOAD_TIME		TIMESTAMP, 	-- tried to load to STG
RAW_PID				BIGINT
);

CREATE TABLE RAW.EXCHANGE_RATES(
DAY 				VARCHAR,
CURRENCY			VARCHAR,	
RATE				VARCHAR,
SRC_FILE 	 		VARCHAR, 	-- FileName
RAW_LOAD_TIME		TIMESTAMP, 	-- loaded to RAW
STG_STATUS			VARCHAR, 	-- 'NEW', 'LOADED', 'ERROR'
STG_LOAD_TIME		TIMESTAMP, 	-- tried to load to STG
RAW_PID				BIGINT
);

CREATE TABLE RAW.PORTS(
PID 				VARCHAR,
CODE				VARCHAR,
SLUG				VARCHAR,
NAME				VARCHAR,
COUNTRY 			VARCHAR,
COUNTRY_CODE		VARCHAR,
SRC_FILE 	 		VARCHAR, 	-- FileName
RAW_LOAD_TIME		TIMESTAMP, 	-- loaded to RAW
STG_STATUS			VARCHAR, 	-- 'NEW', 'LOADED', 'ERROR'
STG_LOAD_TIME		TIMESTAMP, 	-- tried to load to STG
RAW_PID				BIGINT
);

CREATE TABLE RAW.REGIONS(
SLUG				VARCHAR,	
NAME				VARCHAR,	
PARENT				VARCHAR,
SRC_FILE 	 		VARCHAR, 	-- FileName
RAW_LOAD_TIME		TIMESTAMP, 	-- loaded to RAW
STG_STATUS			VARCHAR, 	-- 'NEW', 'LOADED', 'ERROR'
STG_LOAD_TIME		TIMESTAMP, 	-- tried to load to STG
RAW_PID				BIGINT
);


CREATE SCHEMA staging;

CREATE TABLE STAGING.DATAPOINTS(
D_ID				BIGINT,
CREATED				TIMESTAMP,
ORIGIN_PID			INTEGER,
DESTINATION_PID		INTEGER,
VALID_FROM			DATE,
VALID_TO			DATE,
COMPANY_ID			INTEGER,
SUPPLIER_ID			INTEGER,
EQUIPMENT_ID 		INTEGER,
IS_ACTIVE			BOOLEAN
);

CREATE TABLE STAGING.CHARGES(
D_ID 				BIGINT,	
CURRENCY 			VARCHAR,	
CHARGE_VALUE 		DOUBLE
);

CREATE TABLE STAGING.EXCHANGE_RATES(
DAY 				DATE,
CURRENCY			VARCHAR,	
RATE				DOUBLE
);

CREATE TABLE STAGING.PORTS(
PID 				INTEGER,
CODE				VARCHAR,
SLUG				VARCHAR,
NAME				VARCHAR,
COUNTRY 			VARCHAR,
COUNTRY_CODE		VARCHAR
);

CREATE TABLE STAGING.REGIONS(
SLUG				VARCHAR,	
NAME				VARCHAR,	
PARENT				VARCHAR
);



CREATE SCHEMA final;

--prices for datascientist
CREATE VIEW FINAL.PRICES_DS AS
WITH RECURSIVE DATAPOINTS2(d_id, created, origin_pid, destination_pid, company_id, supplier_id, equipment_id, date, valid_to) AS (
    SELECT 
        d_id, 
        created, 
        origin_pid, 
        destination_pid, 
        company_id, 
        supplier_id, 
        equipment_id, 
        valid_from AS date, 
        valid_to 
    FROM 
        staging.datapoints
    WHERE 
        is_active = 1

    UNION ALL

    SELECT 
        d_id, 
        created, 
        origin_pid, 
        destination_pid, 
        company_id, 
        supplier_id, 
        equipment_id, 
        date + INTERVAL 1 DAY, 
        valid_to 
    FROM 
        DATAPOINTS2
    WHERE 
        date < valid_to
), 
agg_cost_by_did AS (
    SELECT 
        dp.d_id, 
        dp.created, 
        dp.origin_pid, 
        dp.destination_pid, 
        dp.company_id, 
        dp.supplier_id, 
        dp.equipment_id, 
        dp.date,
        SUM(c.charge_value / er.rate) AS COST
    FROM  
        DATAPOINTS2 dp
    LEFT JOIN 
        staging.charges c ON dp.d_id = c.d_id
    LEFT JOIN 
        staging.exchange_rates er ON c.CURRENCY = er.CURRENCY AND er.day = dp.date 
    GROUP BY 
        dp.d_id, dp.created, dp.origin_pid, dp.destination_pid, dp.company_id, dp.supplier_id, dp.equipment_id, dp.date
), 
agg_cost_by_ports AS (
    SELECT 
        origin_pid, 
        destination_pid, 
        equipment_id, 
        date, 
        ROUND(MEDIAN(cost), 2) AS med_cost, 
        ROUND(AVG(cost), 2) AS avg_cost, 
        COUNT(DISTINCT supplier_id) AS diff_supplier_cnt, 
        COUNT(DISTINCT company_id) AS diff_company_cnt
    FROM 
        agg_cost_by_did
    GROUP BY 
        origin_pid, destination_pid, equipment_id, date
)
SELECT 
    origin_pid, 
    destination_pid, 
    equipment_id, 
    date, 
    med_cost, 
    avg_cost,
    IF(diff_supplier_cnt >= 2 AND diff_company_cnt >= 5, 1, 0) AS dq_ok
FROM 
    agg_cost_by_ports
ORDER BY 
    origin_pid, destination_pid, equipment_id, date;
	

--prices for customers	
CREATE VIEW FINAL.PRICES AS
select * from FINAL.PRICES_DS where dq_ok = 1	