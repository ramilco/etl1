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

--prices between regions
CREATE VIEW FINAL.PRICES_LOC AS
WITH RECURSIVE all_regions_t AS (
    SELECT 
        SLUG,
        PARENT AS PARENT_SLUG,
        PARENT AS ANCESTOR,
        1 AS LEVEL
    FROM staging.regions
    WHERE PARENT IS NOT NULL 

    UNION ALL

    SELECT 
        child.SLUG,
        parent.PARENT_SLUG,
        parent.ANCESTOR,
        parent.LEVEL + 1
    FROM staging.regions AS child
    JOIN all_regions_t AS parent ON child.PARENT = parent.SLUG
), 
all_regions as (
SELECT SLUG,ANCESTOR FROM all_regions_t
union
SELECT SLUG, SLUG FROM staging.regions where PARENT is NULL
), 
DATAPOINTS2(d_id, created, origin_pid, destination_pid, company_id, supplier_id, equipment_id, date, valid_to) AS (
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
agg_cost_by_did as (
    SELECT 
        dp.d_id,  
        dp.origin_pid, 
        dp.destination_pid, 
        dp.company_id, 
        dp.supplier_id, 
        dp.equipment_id, 
        dp.date,
        SUM(c.charge_value / er.rate) AS COST,
        ar1.slug as origin_slug,
        ar1.ancestor as origin_ancestor,
        ar2.slug as destination_slug,
        ar2.ancestor as destination_ancestor
    FROM  
        DATAPOINTS2 dp
    LEFT JOIN 
        staging.charges c ON dp.d_id = c.d_id
    LEFT JOIN 
        staging.exchange_rates er ON c.CURRENCY = er.CURRENCY AND er.day = dp.date
    LEFT JOIN 
        staging.ports p1 ON dp.origin_pid = p1.pid
    LEFT JOIN 
        all_regions ar1 ON p1.slug = ar1.slug
    LEFT JOIN 
        staging.ports p2 ON dp.destination_pid = p2.pid        
    LEFT JOIN 
        all_regions ar2 ON p2.slug = ar2.slug
    where not exists (select 1 from all_regions ar3 where (ar3.slug = ar1.ancestor and ar3.ancestor = ar2.ancestor)
                                                          or (ar3.slug = ar2.ancestor and ar3.ancestor = ar1.ancestor))    
    group by dp.d_id, dp.origin_pid, dp.destination_pid, dp.company_id, dp.supplier_id, dp.equipment_id, dp.date, 
             ar1.slug, ar1.ancestor, ar2.slug, ar2.ancestor
 ),
agg_cost_by_slugs as(
  SELECT 
        origin_slug as origin, 
        destination_ancestor as destination, 
        equipment_id, 
        date, 
        ROUND(MEDIAN(cost), 2) AS med_cost, 
        ROUND(AVG(cost), 2) AS avg_cost, 
        COUNT(DISTINCT supplier_id) AS diff_supplier_cnt, 
        COUNT(DISTINCT company_id) AS diff_company_cnt
    FROM 
        agg_cost_by_did
    GROUP BY 
        origin_slug, destination_ancestor, equipment_id, date
  UNION  
  SELECT 
        origin_ancestor as origin, 
        destination_slug as destination, 
        equipment_id, 
        date, 
        ROUND(MEDIAN(cost), 2) AS med_cost, 
        ROUND(AVG(cost), 2) AS avg_cost, 
        COUNT(DISTINCT supplier_id) AS diff_supplier_cnt, 
        COUNT(DISTINCT company_id) AS diff_company_cnt
    FROM 
        agg_cost_by_did
    GROUP BY 
        origin_ancestor, destination_slug, equipment_id, date 
  UNION  
  SELECT 
        origin_ancestor as origin, 
        destination_ancestor as destination, 
        equipment_id, 
        date, 
        ROUND(MEDIAN(cost), 2) AS med_cost, 
        ROUND(AVG(cost), 2) AS avg_cost, 
        COUNT(DISTINCT supplier_id) AS diff_supplier_cnt, 
        COUNT(DISTINCT company_id) AS diff_company_cnt
    FROM 
        agg_cost_by_did
    GROUP BY 
        origin_ancestor, destination_ancestor, equipment_id, date  
)
SELECT 
    origin, 
    destination, 
    equipment_id, 
    date, 
    med_cost, 
    avg_cost,
    IF(diff_supplier_cnt >= 2 AND diff_company_cnt >= 5, 1, 0) AS dq_ok
FROM 
    agg_cost_by_slugs
ORDER BY 
    origin, destination, equipment_id, date;

--prices between ports
CREATE VIEW FINAL.PRICES_PORTS AS
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
        p1.code origin, 
        p2.code destination, 
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
    LEFT JOIN 
        staging.ports p1 ON dp.origin_pid = p1.pid
    LEFT JOIN 
        staging.ports p2 ON dp.destination_pid = p2.pid     
    GROUP BY 
        dp.d_id, p1.code, p2.code, dp.company_id, dp.supplier_id, dp.equipment_id, dp.date
), 
agg_cost_by_ports AS (
    SELECT 
        origin, 
        destination, 
        equipment_id, 
        date, 
        ROUND(MEDIAN(cost), 2) AS med_cost, 
        ROUND(AVG(cost), 2) AS avg_cost, 
        COUNT(DISTINCT supplier_id) AS diff_supplier_cnt, 
        COUNT(DISTINCT company_id) AS diff_company_cnt
    FROM 
        agg_cost_by_did
    GROUP BY 
        origin, destination, equipment_id, date
)
SELECT 
    origin, 
    destination, 
    equipment_id, 
    date, 
    med_cost, 
    avg_cost,
    IF(diff_supplier_cnt >= 2 AND diff_company_cnt >= 5, 1, 0) AS dq_ok
FROM 
    agg_cost_by_ports
ORDER BY 
    origin, destination, equipment_id, date;


--prices for datascientist
CREATE OR REPLACE VIEW FINAL.PRICES_DS AS
select * from FINAL.PRICES_LOC
UNION ALL
select * from FINAL.PRICES_PORTS 
	

--prices for customers	
CREATE OR REPLACE VIEW FINAL.PRICES AS
select * from FINAL.PRICES_LOC where dq_ok = 1	
UNION ALL
select * from FINAL.PRICES_PORTS where dq_ok = 1 