ALTER SESSION SET TIMEZONE = 'UTC';

-- Create schema for orchestration db objects
CREATE OR REPLACE SCHEMA STORE_DB.ORCHESTRATION;

USE SCHEMA STORE_DB.ORCHESTRATION;


-- Streams from raw tables
CREATE OR REPLACE STREAM raw_customer_stream
ON TABLE STORE_DB.RAW.CUSTOMER
SHOW_INITIAL_ROWS = FALSE;


CREATE OR REPLACE STREAM raw_store_sales_stream
ON TABLE STORE_DB.RAW.STORE_SALES
SHOW_INITIAL_ROWS = FALSE;


CREATE OR REPLACE STREAM raw_item_stream
ON TABLE STORE_DB.RAW.ITEM
SHOW_INITIAL_ROWS = FALSE;


CREATE OR REPLACE STREAM raw_store_stream
ON TABLE STORE_DB.RAW.STORE
SHOW_INITIAL_ROWS = FALSE;

--------------------------------
SHOW TASKS;
-- SHOW TASKS ILIKE '%root%'
EXECUTE TASK root_process_data;

select SYSTEM$TASK_DEPENDENTS_ENABLE('root_process_data');

ALTER TASK root_process_data SUSPEND;

select * from table(information_schema.task_history())
where name ilike '%PROCESS%'
order by scheduled_time desc;

select * from table(information_schema.task_history())
where name ilike '%PROCESS%' and state = 'FAILED'
order by scheduled_time desc;

select * from table(information_schema.task_history())
where (name ilike '%PROCESS_DIM%' OR name ilike '%PROCESS_FAC%') and state = 'FAILED'
order by scheduled_time desc;

select * from table(information_schema.task_history())
where (name ilike '%PROCESS_DIM%' OR name ilike '%PROCESS_FAC%') and state = 'SUCCEEDED'
order by scheduled_time desc;

select * from table(information_schema.task_history(
    scheduled_time_range_start => dateadd('hour', -4, current_timestamp()),
    result_limit => 5,
    task_name => 'root_process_data'
))
order by scheduled_time desc;



--- All task will be kickstarted after this
CREATE OR REPLACE TASK root_process_data
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
AS
SELECT 1;


-- Tasks Customer
CREATE OR REPLACE TASK task_process_stage_customer
WAREHOUSE = COMPUTE_WH
AFTER root_process_data
AS
MERGE INTO STORE_DB.STAGING.CUSTOMER AS T
USING (
    SELECT
        c_customer_sk AS customer_id,
        c_customer_id AS customer_ext_id,
        c_first_name  AS first_name,
        md5(c_email_address) AS email_address_hash,
        c_last_name   AS last_name,
        c_preferred_cust_flag AS preferred_cust_flag,
        c_birth_day   AS birth_day,
        c_birth_month AS birth_month,
        c_birth_year  AS birth_year,
        C_CREATED_DATE AS created_date,
        METADATA$ACTION AS action
    FROM raw_customer_stream
) AS S
ON T.customer_id = S.customer_id

WHEN MATCHED AND S.action = 'DELETE' THEN
    -- Mark record as deleted instead of physically removing it
    UPDATE SET
        T.is_deleted = TRUE

WHEN MATCHED AND S.action IN ('INSERT','UPDATE') THEN
    -- Overwrite existing record (set is_deleted=FALSE because it's active again)
    UPDATE SET
        T.customer_ext_id      = S.customer_ext_id,
        T.first_name           = S.first_name,
        T.last_name            = S.last_name,
        T.email_address_hash   = S.email_address_hash,
        T.preferred_cust_flag  = S.preferred_cust_flag,
        T.birth_day            = S.birth_day,
        T.birth_month          = S.birth_month,
        T.birth_year           = S.birth_year,
        T.created_date         = S.created_date,
        T.is_deleted           = FALSE,
        T.attribute_hash       = MD5(
                                 COALESCE(S.first_name,'')
                                 || COALESCE(S.last_name,'')
                                 || COALESCE(S.email_address_hash,'')
                                 || COALESCE(S.preferred_cust_flag,'')
                                 || COALESCE(TO_VARCHAR(S.birth_day),'')
                                 || COALESCE(TO_VARCHAR(S.birth_month),'')
                                 || COALESCE(TO_VARCHAR(S.birth_year),'')
                                 || COALESCE(TO_VARCHAR(S.created_date),'')
                               )

WHEN NOT MATCHED AND S.action IN ('INSERT','UPDATE') THEN
    INSERT (
        customer_id,
        customer_ext_id,
        first_name,
        last_name,
        email_address_hash,
        preferred_cust_flag,
        birth_day,
        birth_month,
        birth_year,
        created_date,
        is_deleted,
        attribute_hash
    )
    VALUES (
        S.customer_id,
        S.customer_ext_id,
        S.first_name,
        S.last_name,
        S.email_address_hash,
        S.preferred_cust_flag,
        S.birth_day,
        S.birth_month,
        S.birth_year,
        S.created_date,
        FALSE,
        MD5(
         COALESCE(first_name,'')
           || COALESCE(last_name,'')
           || COALESCE(email_address_hash,'')
           || COALESCE(preferred_cust_flag,'')
           || COALESCE(TO_VARCHAR(birth_day),'')
           || COALESCE(TO_VARCHAR(birth_month),'')
           || COALESCE(TO_VARCHAR(birth_year),'')
           || COALESCE(TO_VARCHAR(CREATED_DATE),'')
        )
    );


-- Task Store
CREATE OR REPLACE TASK task_process_stage_store
WAREHOUSE = COMPUTE_WH
AFTER root_process_data
AS
MERGE INTO STORE_DB.STAGING.STORE T
USING (
    SELECT
        s_store_sk        AS store_id,
        s_store_id        AS store_ext_id,
        s_store_name      AS store_name,
        s_company_name    AS store_company_name,
        s_city            AS store_city,
        s_county          AS store_county,
        s_state           AS store_state,
        s_zip             AS store_zip,
        s_country         AS store_country,
        s_rec_start_date  AS add_date,
        s_rec_end_date    AS discontinued_date,
        METADATA$ACTION   AS action
    FROM raw_store_stream
) S
ON T.store_id = S.store_id

WHEN MATCHED AND S.action = 'DELETE' THEN
    UPDATE SET T.is_deleted = TRUE

WHEN MATCHED AND S.action IN ('INSERT','UPDATE') THEN
    UPDATE SET
        T.store_ext_id        = S.store_ext_id,
        T.store_name          = S.store_name,
        T.store_company_name  = S.store_company_name,
        T.store_city          = S.store_city,
        T.store_county        = S.store_county,
        T.store_state         = S.store_state,
        T.store_zip           = S.store_zip,
        T.store_country       = S.store_country,
        T.add_date            = S.add_date,
        T.discontinued_date   = S.discontinued_date,
        T.is_deleted          = FALSE,
        T.attribute_hash      = MD5(
                                 COALESCE(S.store_id,'')
                               || COALESCE(S.store_name,'')
                               || COALESCE(S.store_company_name,'')
                               || COALESCE(S.store_city,'')
                               || COALESCE(S.store_county,'')
                               || COALESCE(S.store_state,'')
                               || COALESCE(S.store_country,'')
                               || COALESCE(S.store_country,'')
                               || COALESCE(TO_VARCHAR(S.add_date),'')
                               || COALESCE(TO_VARCHAR(S.discontinued_date),'')
                                )

WHEN NOT MATCHED AND S.action IN ('INSERT','UPDATE') THEN
    INSERT (
        store_id,
        store_ext_id,
        store_name,
        store_company_name,
        store_city,
        store_county,
        store_state,
        store_zip,
        store_country,
        add_date,
        discontinued_date,
        is_deleted,
        attribute_hash
    )
    VALUES (
        S.store_id,
        S.store_ext_id,
        S.store_name,
        S.store_company_name,
        S.store_city,
        S.store_county,
        S.store_state,
        S.store_zip,
        S.store_country,
        S.add_date,
        S.discontinued_date,
        FALSE,
        MD5(
         COALESCE(store_id,'')
           || COALESCE(store_name,'')
           || COALESCE(store_company_name,'')
           || COALESCE(store_city,'')
           || COALESCE(store_county,'')
           || COALESCE(store_state,'')
           || COALESCE(store_zip,'')
           || COALESCE(store_country,'')
           || COALESCE(TO_VARCHAR(add_date),'')
           || COALESCE(TO_VARCHAR(discontinued_date),'')
        )
    );



-- Task Item
CREATE OR REPLACE TASK task_process_stage_item
WAREHOUSE = COMPUTE_WH
AFTER root_process_data
AS
MERGE INTO STORE_DB.STAGING.ITEM T
USING (
    SELECT
        i_item_sk         AS item_id,
        i_item_id         AS item_ext_id,
        i_product_name    AS product_name,
        i_category        AS category,
        i_brand           AS brand,
        i_color           AS color,
        i_size            AS size,
        i_rec_start_date  AS add_date,
        i_rec_end_date    AS discontinued_date,
        METADATA$ACTION   AS action
    FROM raw_item_stream
) S
ON T.item_id = S.item_id

WHEN MATCHED AND S.action = 'DELETE' THEN
    UPDATE SET T.is_deleted = TRUE

WHEN MATCHED AND S.action IN ('INSERT','UPDATE') THEN
    UPDATE SET
        T.item_ext_id       = S.item_ext_id,
        T.product_name      = S.product_name,
        T.category          = S.category,
        T.brand             = S.brand,
        T.color             = S.color,
        T.size              = S.size,
        T.add_date          = S.add_date,
        T.discontinued_date = S.discontinued_date,
        T.is_deleted        = FALSE

WHEN NOT MATCHED AND S.action IN ('INSERT','UPDATE') THEN
    INSERT (
        item_id,
        item_ext_id,
        product_name,
        category,
        brand,
        color,
        size,
        add_date,
        discontinued_date,
        is_deleted
    )
    VALUES (
        S.item_id,
        S.item_ext_id,
        S.product_name,
        S.category,
        S.brand,
        S.color,
        S.size,
        S.add_date,
        S.discontinued_date,
        FALSE
    );



-- Tasks Store Sales
CREATE OR REPLACE TASK task_process_stage_store_sales
WAREHOUSE = COMPUTE_WH
AFTER root_process_data
AS
MERGE INTO STORE_DB.STAGING.STORE_SALES AS T
USING (
    SELECT
        ss_ticket_number      AS ticket_number,
        ss_customer_sk        AS customer_id,
        ss_store_sk           AS store_id,
        ss_item_sk            AS item_id,
        ss_quantity           AS quantity,
        ss_sales_price        AS sales_price,
        ss_ext_sales_price    AS ext_sales_price,
        ss_coupon_amt         AS coupon_amt,
        ss_net_paid           AS paid_amount,
        ss_sold_date          AS sold_date,    -- if needed
        CONCAT(CAST(SS_SOLD_DATE AS STRING), ' ', SS_SOLD_TIME)::timestamp AS sold_at,
        METADATA$ACTION       AS action
    FROM raw_store_sales_stream
) S
ON T.ticket_number = S.ticket_number

WHEN MATCHED AND S.action = 'DELETE' THEN
    UPDATE SET T.is_deleted = TRUE

WHEN MATCHED AND S.action IN ('INSERT','UPDATE') THEN
    UPDATE SET
        T.customer_id       = S.customer_id,
        T.store_id          = S.store_id,
        T.item_id           = S.item_id,
        T.quantity          = S.quantity,
        T.sales_price       = S.sales_price,
        T.ext_sales_price   = S.ext_sales_price,
        T.coupon_amt        = S.coupon_amt,
        T.paid_amount       = S.paid_amount,
        T.sold_at           = S.sold_at,
        T.is_deleted        = FALSE

WHEN NOT MATCHED AND S.action IN ('INSERT','UPDATE') THEN
    INSERT (
        ticket_number,
        customer_id,
        store_id,
        item_id,
        quantity,
        sales_price,
        ext_sales_price,
        coupon_amt,
        paid_amount,
        sold_at,
        is_deleted
    )
    VALUES (
        S.ticket_number,
        S.customer_id,
        S.store_id,
        S.item_id,
        S.quantity,
        S.sales_price,
        S.ext_sales_price,
        S.coupon_amt,
        S.paid_amount,
        S.sold_at,
        FALSE
    );

