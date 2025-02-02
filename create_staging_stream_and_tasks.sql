USE SCHEMA STORE_DB.ORCHESTRATION;

CREATE OR REPLACE STREAM staging_customer_stream
ON TABLE STORE_DB.STAGING.CUSTOMER;

CREATE OR REPLACE STREAM staging_store_sales_stream
ON TABLE STORE_DB.STAGING.STORE_SALES;

CREATE OR REPLACE STREAM staging_item_stream
ON TABLE STORE_DB.STAGING.ITEM;

CREATE OR REPLACE STREAM staging_store_stream
ON TABLE STORE_DB.STAGING.STORE;


-- TASK to merge into dim_customer from staging
CREATE OR REPLACE TASK task_process_dim_customer -- scd2
WAREHOUSE = COMPUTE_WH
AFTER task_process_stage_customer
AS
MERGE INTO STORE_DB.DATA.DIM_CUSTOMER AS T
USING (
    SELECT
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
        attribute_hash,
        METADATA$ACTION AS action
    FROM staging_customer_stream
) AS S
ON T.customer_id = S.customer_id
   AND T.current_flag = TRUE -- match only current "active" record

-- 1) Handle logical deletes:
WHEN MATCHED AND S.is_deleted = TRUE THEN
    UPDATE SET
        current_flag       = FALSE,
        to_dt = CURRENT_TIMESTAMP

-- 2) If not deleted, check for attribute changes (compare hashes):
--    If the hash changed, we expire the old record. The "INSERT" clause below
--    (WHEN NOT MATCHED) will add the new row.
WHEN MATCHED
     AND S.is_deleted = FALSE
     AND T.attribute_hash != S.attribute_hash
THEN
    UPDATE SET
        current_flag       = FALSE,
        to_dt = CURRENT_TIMESTAMP

-- 3) Insert a new row if not matched or if old row was just expired:
WHEN NOT MATCHED
     AND S.is_deleted = FALSE
THEN
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
        from_dt,
        to_dt,
        current_flag,
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
        CURRENT_TIMESTAMP,  -- new version starts now
        NULL,               -- no end date (active)
        TRUE,               -- mark it as current
        S.attribute_hash
    );


-- TASK Dim store
CREATE OR REPLACE TASK task_process_dim_store -- scd2
WAREHOUSE = COMPUTE_WH
AFTER task_process_stage_store
AS
MERGE INTO STORE_DB.DATA.DIM_STORE AS T
USING (
    SELECT
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
        attribute_hash,
        METADATA$ACTION AS action
    FROM staging_store_stream
) AS S
ON T.store_id = S.store_id
   AND T.current_flag = TRUE    -- match only the active version

WHEN MATCHED AND S.is_deleted = TRUE THEN
    -- Expire the active version if it's flagged as deleted
    UPDATE SET
        current_flag = FALSE,
        to_dt = CURRENT_TIMESTAMP

WHEN MATCHED
     AND S.is_deleted = FALSE
     AND T.attribute_hash != S.attribute_hash
THEN
    -- Attributes changed => expire old record
    UPDATE SET
        current_flag = FALSE,
        to_dt = CURRENT_TIMESTAMP

WHEN NOT MATCHED
     AND S.is_deleted = FALSE
THEN
    -- Insert a new active version
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
        from_dt,
        to_dt,
        current_flag,
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
        CURRENT_TIMESTAMP,     -- new version starts now
        NULL,                  -- no end date, still active
        TRUE,                  -- current version
        S.attribute_hash
    );


--- Task Item

CREATE OR REPLACE TASK task_process_dim_item -- scd1
WAREHOUSE = COMPUTE_WH
AFTER task_process_stage_item
AS
MERGE INTO STORE_DB.DATA.DIM_ITEM T
USING (
    SELECT
        i.item_id,
        i.item_ext_id,
        i.product_name,
        i.category,
        i.brand,
        i.color,
        i.size,
        i.add_date,
        i.discontinued_date,
        METADATA$ACTION AS ACTION
    FROM staging_item_stream i
) S
ON T.item_id = S.item_id

WHEN MATCHED AND S.ACTION = 'DELETE' THEN
    DELETE  -- physically remove the record

WHEN MATCHED AND S.ACTION IN ('UPDATE','INSERT') THEN
    -- Overwrite existing data
    UPDATE SET
        T.item_ext_id       = S.item_ext_id,
        T.product_name      = S.product_name,
        T.category          = S.category,
        T.brand             = S.brand,
        T.color             = S.color,
        T.size              = S.size,
        T.add_date          = S.add_date,
        T.discontinued_date = S.discontinued_date

WHEN NOT MATCHED AND S.ACTION IN ('UPDATE','INSERT') THEN
    INSERT (
        item_id,
        item_ext_id,
        product_name,
        category,
        brand,
        color,
        size,
        add_date,
        discontinued_date
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
        S.discontinued_date
    );


-- Task store sales
CREATE OR REPLACE TASK task_process_fact_store_sales
WAREHOUSE = COMPUTE_WH
AFTER task_process_dim_customer, task_process_dim_store, task_process_stage_store_sales
AS
MERGE INTO STORE_DB.DATA.FACT_STORE_SALES AS T
USING (
    SELECT
        ticket_number,
        customer_id,
        item_id,
        store_id,
        quantity,
        sales_price,
        ext_sales_price,
        coupon_amt,
        paid_amount,
        sold_at,
        EXTRACT(YEAR FROM sold_at)   AS sale_year,
        EXTRACT(MONTH FROM sold_at)  AS sale_month,
        EXTRACT(DAY FROM sold_at)    AS sale_day,
        EXTRACT(HOUR FROM sold_at)   AS sale_hour,
        EXTRACT(MINUTE FROM sold_at) AS sale_minute,
        is_deleted,
        METADATA$ACTION AS action
    FROM staging_store_sales_stream
) AS S
ON T.ticket_number = S.ticket_number

-- 1) Physically remove rows from FACT if staging says 'is_deleted=TRUE'.
WHEN MATCHED AND S.is_deleted = TRUE THEN
    DELETE

-- 2) Update existing rows if they match and 'is_deleted=FALSE'.
WHEN MATCHED AND S.is_deleted = FALSE THEN
    UPDATE SET
        T.customer_id     = S.customer_id,
        T.item_id         = S.item_id,
        T.store_id        = S.store_id,
        T.quantity        = S.quantity,
        T.sales_price     = S.sales_price,
        T.ext_sales_price = S.ext_sales_price,
        T.coupon_amt      = S.coupon_amt,
        T.paid_amount     = S.paid_amount,
        T.sold_at         = S.sold_at,
        T.sale_year       = S.sale_year,
        T.sale_month      = S.sale_month,
        T.sale_day        = S.sale_day,
        T.sale_hour       = S.sale_hour,
        T.sale_minute     = S.sale_minute

-- 3) Insert new rows if not matched and 'is_deleted=FALSE'.
WHEN NOT MATCHED AND S.is_deleted = FALSE THEN
    INSERT (
        ticket_number,
        customer_id,
        item_id,
        store_id,
        quantity,
        sales_price,
        ext_sales_price,
        coupon_amt,
        paid_amount,
        sold_at,
        sale_year,
        sale_month,
        sale_day,
        sale_hour,
        sale_minute
    )
    VALUES (
        S.ticket_number,
        S.customer_id,
        S.item_id,
        S.store_id,
        S.quantity,
        S.sales_price,
        S.ext_sales_price,
        S.coupon_amt,
        S.paid_amount,
        S.sold_at,
        S.sale_year,
        S.sale_month,
        S.sale_day,
        S.sale_hour,
        S.sale_minute
    );
