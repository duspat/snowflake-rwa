ALTER SESSION SET TIMEZONE = 'UTC';

USE SCHEMA STORE_DB.ORCHESTRATION;

-- Generate Customer data store procedure
CREATE OR REPLACE PROCEDURE SP_GENERATE_CUSTOMER()
RETURNS INT
LANGUAGE SQL
AS
DECLARE
    customer_id NUMBER;
BEGIN
    -- SELECT CAST(UNIFORM(9999999, 99999999, RANDOM()) AS NUMBER(38,0))
    --   INTO :customer_id;

    SELECT MAX(C_CUSTOMER_SK) + 1 INTO :customer_id FROM STORE_DB.RAW.CUSTOMER;

    INSERT INTO STORE_DB.RAW.CUSTOMER
      (
        C_CUSTOMER_SK,
        C_CUSTOMER_ID,
        C_FIRST_NAME,
        C_LAST_NAME,
        C_FIRST_SALES_DATE_SK,
        C_CREATED_DATE
      )
      SELECT
        :customer_id AS C_CUSTOMER_SK,
        'NC_' || CAST(:customer_id AS STRING)  AS C_CUSTOMER_ID,
        'FName_'    || CAST(:customer_id AS STRING)  AS C_FIRST_NAME,
        'LName_'    || CAST(:customer_id AS STRING)  AS C_LAST_NAME,
        CAST(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD') AS NUMBER(8,0))        AS C_FIRST_SALES_DATE_SK,
        CURRENT_DATE() AS C_CREATED_DATE
      FROM TABLE(GENERATOR(ROWCOUNT => 1));

    RETURN customer_id;
END;

---------------------------
CALL SP_GENERATE_CUSTOMER();
---------------------------

-- Store procedure to generate random sales
CREATE OR REPLACE PROCEDURE SP_GENERATE_STORE_SALES(
    create_new_customer boolean DEFAULT FALSE,
    customer_id NUMBER DEFAULT NULL
    )
RETURNS OBJECT
LANGUAGE SQL
AS
DECLARE
    CUSTOMER_ID_EX EXCEPTION (-20003, 'Supplied customer_id does not exist!.');
    ticket_id NUMBER;
BEGIN
    IF (create_new_customer) THEN
        -- Create new customer into CUSTOMER table
        customer_id := (CALL SP_GENERATE_CUSTOMER());
    ELSEIF (customer_id IS NULL) THEN
        -- Random existing customer from CUSTOMER table
        customer_id := (SELECT C_CUSTOMER_SK FROM STORE_DB.RAW.CUSTOMER SAMPLE ROW (1 ROWS));
    END IF;

    IF (customer_id IS NOT NULL) THEN
        -- Check if supplied customer_id in STORE_DB.RAW.CUSTOMER table
        SELECT C_CUSTOMER_SK INTO :customer_id FROM STORE_DB.RAW.CUSTOMER WHERE C_CUSTOMER_SK = :customer_id;
        IF (customer_id IS NULL) THEN
            RAISE CUSTOMER_ID_EX;
        END IF;
    END IF;

    SELECT MAX(SS_TICKET_NUMBER) + 1 INTO :ticket_id FROM STORE_DB.RAW.STORE_SALES;


    INSERT INTO STORE_DB.RAW.STORE_SALES
    (
      SS_SOLD_DATE_SK,
      SS_SOLD_TIME_SK,
      SS_ITEM_SK,
      SS_CUSTOMER_SK,
      SS_CDEMO_SK,
      SS_HDEMO_SK,
      SS_ADDR_SK,
      SS_STORE_SK,
      SS_PROMO_SK,
      SS_TICKET_NUMBER,
      SS_QUANTITY,
      SS_WHOLESALE_COST,
      SS_LIST_PRICE,
      SS_SALES_PRICE,
      SS_EXT_DISCOUNT_AMT,
      SS_EXT_SALES_PRICE,
      SS_EXT_WHOLESALE_COST,
      SS_EXT_LIST_PRICE,
      SS_EXT_TAX,
      SS_COUPON_AMT,
      SS_NET_PAID,
      SS_NET_PAID_INC_TAX,
      SS_NET_PROFIT,
      SS_SOLD_DATE,
      SS_SOLD_TIME
    )
    SELECT
      -- Generate random date/time SKs
      CAST(UNIFORM(10000,99999,RANDOM()) AS NUMBER(38,0)) AS SS_SOLD_DATE_SK,
      CAST(UNIFORM(10000,99999,RANDOM()) AS NUMBER(38,0)) AS SS_SOLD_TIME_SK,

      -- Random item from ITEM table
      (
        SELECT I_ITEM_SK
        FROM STORE_DB.RAW.ITEM
        SAMPLE ROW (1 ROWS)
      ) AS SS_ITEM_SK,

      :customer_id AS SS_CUSTOMER_SK,

      -- Misc. demographics or addresses
      CAST(UNIFORM(1,5000,RANDOM()) AS NUMBER(38,0)) AS SS_CDEMO_SK,
      CAST(UNIFORM(1,5000,RANDOM()) AS NUMBER(38,0)) AS SS_HDEMO_SK,
      CAST(UNIFORM(1,5000,RANDOM()) AS NUMBER(38,0)) AS SS_ADDR_SK,
      CAST(UNIFORM(1,5000,RANDOM()) AS NUMBER(38,0)) AS SS_STORE_SK,
      CAST(UNIFORM(1,5000,RANDOM()) AS NUMBER(38,0)) AS SS_PROMO_SK,

      :ticket_id AS SS_TICKET_NUMBER,

      -- Sales metrics
      CAST(UNIFORM(1,10,RANDOM()) AS NUMBER(7,2)) AS SS_QUANTITY,
      CAST(UNIFORM(1,50,RANDOM()) AS NUMBER(7,2)) AS SS_WHOLESALE_COST,
      CAST(UNIFORM(1,100,RANDOM()) AS NUMBER(7,2)) AS SS_LIST_PRICE,
      CAST(UNIFORM(1,100,RANDOM()) AS NUMBER(7,2)) AS SS_SALES_PRICE,
      CAST(UNIFORM(0,10,RANDOM()) AS NUMBER(7,2)) AS SS_EXT_DISCOUNT_AMT,
      CAST(UNIFORM(1,300,RANDOM()) AS NUMBER(7,2)) AS SS_EXT_SALES_PRICE,
      CAST(UNIFORM(1,50,RANDOM()) AS NUMBER(7,2)) AS SS_EXT_WHOLESALE_COST,
      CAST(UNIFORM(1,300,RANDOM()) AS NUMBER(7,2)) AS SS_EXT_LIST_PRICE,
      CAST(UNIFORM(0,20,RANDOM()) AS NUMBER(7,2)) AS SS_EXT_TAX,
      CAST(UNIFORM(0,10,RANDOM()) AS NUMBER(7,2)) AS SS_COUPON_AMT,
      CAST(UNIFORM(1,300,RANDOM()) AS NUMBER(7,2)) AS SS_NET_PAID,
      CAST(UNIFORM(1,320,RANDOM()) AS NUMBER(7,2)) AS SS_NET_PAID_INC_TAX,
      CAST(UNIFORM(1,300,RANDOM()) AS NUMBER(7,2)) AS SS_NET_PROFIT,

      -- Date/time for convenience
      CURRENT_DATE()                            AS SS_SOLD_DATE,
      TO_CHAR(CURRENT_TIME(), 'HH24:MI:SS')     AS SS_SOLD_TIME
    FROM TABLE(GENERATOR(ROWCOUNT => 1));



    RETURN OBJECT_CONSTRUCT(
        'customer_id', customer_id,
        'ticket_id', ticket_id,
        'create_new_customer', create_new_customer
    );
END;

------------------------------------------------
CALL SP_GENERATE_STORE_SALES(TRUE);
CALL SP_GENERATE_STORE_SALES(FALSE, 31104554);
CALL SP_GENERATE_STORE_SALES();
CALL SP_GENERATE_STORE_SALES(FALSE, -1);

SELECT COUNT(*) FROM STORE_DB.RAW.STORE_SALES;

SELECT * FROM STORE_DB.RAW.STORE_SALES WHERE SS_CUSTOMER_SK IN (31104554, 57584515, 37621353);
SELECT * FROM STORE_DB.RAW.CUSTOMER ORDER BY C_CREATED_DATE DESC LIMIT 5;
----------------------------------------------------------------------------

-- DROP PROCEDURE GENERATE_STORE_SALES(NUMBER, BOOLEAN);

--- Store procedure to generate random store sales and customer data
CREATE OR REPLACE PROCEDURE SP_GENERATE_RANDOM_STORE_SALES(
    no_of_records NUMBER DEFAULT 10,
    create_new_customer boolean DEFAULT FALSE,
    customer_id NUMBER DEFAULT NULL
    )
RETURNS TABLE(customer_id number, ticket_id number, create_new_customer boolean)
LANGUAGE SQL
AS
DECLARE
    created_records_count INT DEFAULT 1;
    result_objects ARRAY DEFAULT ARRAY_CONSTRUCT();
    result_object OBJECT;
    results RESULTSET;
BEGIN
    WHILE (created_records_count <= no_of_records)
    DO
        result_object := (CALL SP_GENERATE_STORE_SALES(:create_new_customer, :customer_id));
        result_objects := ARRAY_APPEND(result_objects, result_object);
        created_records_count := created_records_count + 1;
    END WHILE;

    results := (SELECT
          value:customer_id::NUMBER AS customer_id,
          value:ticket_id::NUMBER AS ticket_id,
          value:create_new_customer::BOOLEAN AS create_new_customer
      FROM TABLE(FLATTEN(INPUT => :result_objects)));

    RETURN table(results);
END;

-----------------------------------------------------
CALL SP_GENERATE_RANDOM_STORE_SALES(1);

CALL SP_GENERATE_RANDOM_STORE_SALES(2, TRUE);

CALL SP_GENERATE_RANDOM_STORE_SALES(2, FALSE, 64999990);

SELECT COUNT(*) FROM STORE_DB.RAW.STORE_SALES;

SELECT COUNT(*) FROM STORE_DB.STAGING.STORE_SALES;

SELECT COUNT(*) FROM STORE_DB.DATA.FACT_STORE_SALES; -- 7913175

SELECT COUNT(*) FROM STORE_DB.DATA.DIM_CUSTOMER;

DESC TABLE STORE_DB.RAW.STORE_SALES;
-----------------------------------------------------
