-- Declare variable for the current date
DECLARE RUN_DATE DATE DEFAULT CURRENT_DATE();  -- Run-time "today" (2025-05-23)

-- Helper function to mimic EOMONTH (end of month after adding months)
CREATE TEMP FUNCTION EOMONTH(start_date DATE, months INT64) AS (
  DATE_SUB(DATE_TRUNC(DATE_ADD(start_date, INTERVAL months MONTH), MONTH), INTERVAL 1 DAY)
);

WITH tx AS (
  -- Step 1: Extract all transaction data
  SELECT
    indiv_id,
    DATE(txn_dt) AS tx_date,
    prch_chnl_cd,
    'TOTAL' AS total_key
  FROM `mtech-daas-transact-sdata.rfnd_sls.merch_all`
  WHERE prch_chnl_cd IN ('A', 'F')
    AND fin_own_lease_ind = 'Y'
    AND rtl_divn_nbr IN (71, 77)
    AND pdiv_id NOT IN ('000', '065')
    AND dept_vnd_cd IS NOT NULL
    AND indiv_id IS NOT NULL
    AND LENGTH(CAST(indiv_id AS STRING)) >= 8
    AND indiv_id != 1
    -- AND indiv_id = 10018809  -- Uncomment for testing
  GROUP BY indiv_id, tx_date, prch_chnl_cd, total_key
),

purchases AS (
  -- Step 2: Get all distinct purchase dates per customer
  SELECT DISTINCT
    indiv_id,
    tx_date
  FROM tx
),

period_starts AS (
  -- Step 3: Generate period start dates based on purchases
  SELECT
    indiv_id,
    tx_date AS period_start,
    ROW_NUMBER() OVER (PARTITION BY indiv_id ORDER BY tx_date) AS period_num
  FROM purchases
),

date_periods AS (
  -- Step 4: Generate periods dynamically
  SELECT
    p.indiv_id,
    p.period_start AS greg_strt_dt,
    COALESCE(
      LEAST(
        EOMONTH(p.period_start, 12),
        RUN_DATE
      ),
      RUN_DATE
    ) AS greg_end_dt,
    p.period_num,
    LEAD(p.period_start) OVER (PARTITION BY p.indiv_id ORDER BY p.period_start) AS next_period_start
  FROM period_starts p
),

adjusted_periods AS (
  -- Step 5: Adjust periods to fill gaps and cover up to RUN_DATE
  SELECT
    indiv_id,
    greg_strt_dt,
    greg_end_dt
  FROM date_periods
  UNION ALL
  -- Add intermediate periods for gaps
  SELECT
    dp.indiv_id,
    DATE_ADD(dp.greg_end_dt, INTERVAL 1 DAY) AS greg_strt_dt,
    COALESCE(
      LEAST(
        EOMONTH(DATE_ADD(dp.greg_end_dt, INTERVAL 1 DAY), 12),
        RUN_DATE
      ),
      RUN_DATE
    ) AS greg_end_dt
  FROM date_periods dp
  WHERE dp.next_period_start IS NOT NULL
    AND DATE_ADD(dp.greg_end_dt, INTERVAL 1 DAY) < dp.next_period_start
    AND DATE_ADD(dp.greg_end_dt, INTERVAL 1 DAY) <= RUN_DATE
),

customer_dates AS (
  -- Step 6: Final periods dataset
  SELECT
    indiv_id,
    greg_strt_dt,
    greg_end_dt
  FROM adjusted_periods
  WHERE greg_end_dt <= RUN_DATE
),

identified_txn_dates_total AS (
  -- Step 7: Calculate transaction dates for Total channel
  SELECT
    indiv_id,
    tx_date,
    COALESCE(nxt_tx_date, DATE '3499-12-31') AS nxt_tx_dt,
    first_purchase,
    last_purchase
  FROM (
    SELECT
      indiv_id,
      tx_date,
      MIN(tx_date) OVER (PARTITION BY indiv_id) AS first_purchase,
      MAX(tx_date) OVER (PARTITION BY indiv_id) AS last_purchase,
      LEAD(tx_date) OVER (PARTITION BY indiv_id ORDER BY tx_date) AS nxt_tx_date
    FROM purchases
  ) DRV_2
  ORDER BY tx_date
),

identified_txn_dates_mcom AS (
  -- Step 8: Calculate transaction dates for MCOM (E-Commerce) channel
  SELECT
    indiv_id,
    tx_date_mcom,
    COALESCE(nxt_tx_date_mcom, DATE '3499-12-31') AS nxt_tx_dt_mcom,
    first_purchase_mcom,
    last_purchase_mcom
  FROM (
    SELECT
      indiv_id,
      tx_date_mcom,
      MIN(tx_date_mcom) OVER (PARTITION BY indiv_id) AS first_purchase_mcom,
      MAX(tx_date_mcom) OVER (PARTITION BY indiv_id) AS last_purchase_mcom,
      LEAD(tx_date_mcom) OVER (PARTITION BY indiv_id ORDER BY tx_date_mcom) AS nxt_tx_date_mcom
    FROM (
      SELECT DISTINCT indiv_id, tx_date AS tx_date_mcom
      FROM tx
      WHERE prch_chnl_cd = 'F'
    ) DRV_1
  ) DRV_2
  ORDER BY tx_date_mcom
),

identified_txn_dates_store AS (
  -- Step 9: Calculate transaction dates for Store channel
  SELECT
    indiv_id,
    tx_date_store,
    COALESCE(nxt_tx_date_store, DATE '3499-12-31') AS nxt_tx_dt_store,
    first_purchase_store,
    last_purchase_store
  FROM (
    SELECT
      indiv_id,
      tx_date_store,
      MIN(tx_date_store) OVER (PARTITION BY indiv_id) AS first_purchase_store,
      MAX(tx_date_store) OVER (PARTITION BY indiv_id) AS last_purchase_store,
      LEAD(tx_date_store) OVER (PARTITION BY indiv_id ORDER BY tx_date_store) AS nxt_tx_date_store
    FROM (
      SELECT DISTINCT indiv_id, tx_date AS tx_date_store
      FROM tx
      WHERE prch_chnl_cd = 'A'
    ) DRV_1
  ) DRV_2
  ORDER BY tx_date_store
),

roll AS (
  -- Step 10: Combine transaction data with periods
  SELECT
    cd.indiv_id,
    cd.greg_strt_dt,
    cd.greg_end_dt,
    tx.tx_date,
    tx.nxt_tx_dt,
    tx.first_purchase,
    tx.last_purchase,
    tx_mcom.tx_date_mcom,
    tx_mcom.nxt_tx_dt_mcom,
    tx_mcom.first_purchase_mcom,
    tx_mcom.last_purchase_mcom,
    tx_store.tx_date_store,
    tx_store.nxt_tx_dt_store,
    tx_store.first_purchase_store,
    tx_store.last_purchase_store
  FROM customer_dates cd
  LEFT JOIN identified_txn_dates_total tx 
    ON tx.indiv_id = cd.indiv_id
    AND cd.greg_end_dt BETWEEN tx.tx_date AND tx.nxt_tx_dt
  LEFT JOIN identified_txn_dates_mcom tx_mcom 
    ON tx_mcom.indiv_id = cd.indiv_id
    AND cd.greg_end_dt BETWEEN tx_mcom.tx_date_mcom AND tx_mcom.nxt_tx_dt_mcom
  LEFT JOIN identified_txn_dates_store tx_store 
    ON tx_store.indiv_id = cd.indiv_id
    AND cd.greg_end_dt BETWEEN tx_store.tx_date_store AND tx_store.nxt_tx_dt_store
),

purchases_in_period AS (
  -- Step 11: Identify purchases within each period
  SELECT
    cd.indiv_id,
    cd.greg_strt_dt,
    cd.greg_end_dt,
    MAX(CASE WHEN tx.prch_chnl_cd IN ('A', 'F') THEN tx.tx_date END) AS latest_purchase_in_period_total,
    MAX(CASE WHEN tx.prch_chnl_cd = 'F' THEN tx.tx_date END) AS latest_purchase_in_period_mcom,
    MAX(CASE WHEN tx.prch_chnl_cd = 'A' THEN tx.tx_date END) AS latest_purchase_in_period_store
  FROM customer_dates cd
  LEFT JOIN tx 
    ON tx.indiv_id = cd.indiv_id
    AND tx.tx_date BETWEEN cd.greg_strt_dt AND cd.greg_end_dt
  GROUP BY cd.indiv_id, cd.greg_strt_dt, cd.greg_end_dt
),

labeled AS (
  -- Step 12: Assign customer status and calculate flags
  SELECT
    r.indiv_id,
    r.greg_strt_dt,
    r.greg_end_dt,
    r.last_purchase AS last_purchase_dt_total,
    r.last_purchase_mcom AS last_purchase_dt_mcom,
    r.last_purchase_store AS last_purchase_dt_store,
    p.latest_purchase_in_period_total,
    p.latest_purchase_in_period_mcom,
    p.latest_purchase_in_period_store,
    -- Total channel status
    CASE
      WHEN r.first_purchase = r.greg_strt_dt THEN 'NET NEW'
      WHEN p.latest_purchase_in_period_total IS NOT NULL 
        AND LAG(r.last_purchase, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt) IS NOT NULL
        AND DATE_DIFF(p.latest_purchase_in_period_total, 
                      LAG(r.last_purchase, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt), 
                      MONTH) >= 12
        AND DATE_DIFF(p.latest_purchase_in_period_total, 
                      LAG(r.last_purchase, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt), 
                      MONTH) <= 24 THEN 'REACTIVE'
      WHEN p.latest_purchase_in_period_total IS NOT NULL 
        AND LAG(r.last_purchase, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt) IS NOT NULL
        AND DATE_DIFF(p.latest_purchase_in_period_total, 
                      LAG(r.last_purchase, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt), 
                      MONTH) >= 12 THEN 'NEW'
      WHEN DATE_DIFF(r.greg_end_dt, r.last_purchase, MONTH) <= 12 THEN 'RETAINED'
      WHEN DATE_DIFF(r.greg_end_dt, r.last_purchase, MONTH) BETWEEN 13 AND 24 THEN 'LAPSED'
      WHEN DATE_DIFF(r.greg_end_dt, r.last_purchase, MONTH) > 24 AND r.first_purchase IS NOT NULL THEN 'INACTIVE'
      ELSE NULL
    END AS total_cs,
    -- MCOM channel status
    CASE
      WHEN r.first_purchase_mcom = r.greg_strt_dt THEN 'NET NEW'
      WHEN p.latest_purchase_in_period_mcom IS NOT NULL 
        AND LAG(r.last_purchase_mcom, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt) IS NOT NULL
        AND DATE_DIFF(p.latest_purchase_in_period_mcom, 
                      LAG(r.last_purchase_mcom, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt), 
                      MONTH) >= 12
        AND DATE_DIFF(p.latest_purchase_in_period_mcom, 
                      LAG(r.last_purchase_mcom, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt), 
                      MONTH) <= 24 THEN 'REACTIVE'
      WHEN p.latest_purchase_in_period_mcom IS NOT NULL 
        AND LAG(r.last_purchase_mcom, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt) IS NOT NULL
        AND DATE_DIFF(p.latest_purchase_in_period_mcom, 
                      LAG(r.last_purchase_mcom, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt), 
                      MONTH) >= 12 THEN 'NEW'
      WHEN DATE_DIFF(r.greg_end_dt, r.last_purchase_mcom, MONTH) <= 12 THEN 'RETAINED'
      WHEN DATE_DIFF(r.greg_end_dt, r.last_purchase_mcom, MONTH) BETWEEN 13 AND 24 THEN 'LAPSED'
      WHEN DATE_DIFF(r.greg_end_dt, r.last_purchase_mcom, MONTH) > 24 AND r.first_purchase_mcom IS NOT NULL THEN 'INACTIVE'
      ELSE NULL
    END AS mcom_cs,
    -- Store channel status
    CASE
      WHEN r.first_purchase_store = r.greg_strt_dt THEN 'NET NEW'
      WHEN p.latest_purchase_in_period_store IS NOT NULL 
        AND LAG(r.last_purchase_store, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt) IS NOT NULL
        AND DATE_DIFF(p.latest_purchase_in_period_store, 
                      LAG(r.last_purchase_store, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt), 
                      MONTH) >= 12
        AND DATE_DIFF(p.latest_purchase_in_period_store, 
                      LAG(r.last_purchase_store, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt), 
                      MONTH) <= 24 THEN 'REACTIVE'
      WHEN p.latest_purchase_in_period_store IS NOT NULL 
        AND LAG(r.last_purchase_store, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt) IS NOT NULL
        AND DATE_DIFF(p.latest_purchase_in_period_store, 
                      LAG(r.last_purchase_store, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt), 
                      MONTH) >= 12 THEN 'NEW'
      WHEN DATE_DIFF(r.greg_end_dt, r.last_purchase_store, MONTH) <= 12 THEN 'RETAINED'
      WHEN DATE_DIFF(r.greg_end_dt, r.last_purchase_store, MONTH) BETWEEN 13 AND 24 THEN 'LAPSED'
      WHEN DATE_DIFF(r.greg_end_dt, r.last_purchase_store, MONTH) > 24 AND r.first_purchase_store IS NOT NULL THEN 'INACTIVE'
      ELSE NULL
    END AS store_cs,
    -- Application flag: Y if active status in any channel, else N
    CASE
      WHEN COALESCE(
        CASE
          WHEN r.first_purchase = r.greg_strt_dt THEN 'NET NEW'
          WHEN p.latest_purchase_in_period_total IS NOT NULL 
            AND LAG(r.last_purchase, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt) IS NOT NULL
            AND DATE_DIFF(p.latest_purchase_in_period_total, 
                          LAG(r.last_purchase, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt), 
                          MONTH) >= 12
            AND DATE_DIFF(p.latest_purchase_in_period_total, 
                          LAG(r.last_purchase, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt), 
                          MONTH) <= 24 THEN 'REACTIVE'
          WHEN p.latest_purchase_in_period_total IS NOT NULL 
            AND LAG(r.last_purchase, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt) IS NOT NULL
            AND DATE_DIFF(p.latest_purchase_in_period_total, 
                          LAG(r.last_purchase, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt), 
                          MONTH) >= 12 THEN 'NEW'
          WHEN DATE_DIFF(r.greg_end_dt, r.last_purchase, MONTH) <= 12 THEN 'RETAINED'
          WHEN DATE_DIFF(r.greg_end_dt, r.last_purchase, MONTH) BETWEEN 13 AND 24 THEN 'LAPSED'
          WHEN DATE_DIFF(r.greg_end_dt, r.last_purchase, MONTH) > 24 AND r.first_purchase IS NOT NULL THEN 'INACTIVE'
          ELSE NULL
        END,
        CASE
          WHEN r.first_purchase_mcom = r.greg_strt_dt THEN 'NET NEW'
          WHEN p.latest_purchase_in_period_mcom IS NOT NULL 
            AND LAG(r.last_purchase_mcom, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt) IS NOT NULL
            AND DATE_DIFF(p.latest_purchase_in_period_mcom, 
                          LAG(r.last_purchase_mcom, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt), 
                          MONTH) >= 12
            AND DATE_DIFF(p.latest_purchase_in_period_mcom, 
                          LAG(r.last_purchase_mcom, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt), 
                          MONTH) <= 24 THEN 'REACTIVE'
          WHEN p.latest_purchase_in_period_mcom IS NOT NULL 
            AND LAG(r.last_purchase_mcom, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt) IS NOT NULL
            AND DATE_DIFF(p.latest_purchase_in_period_mcom, 
                          LAG(r.last_purchase_mcom, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt), 
                          MONTH) >= 12 THEN 'NEW'
          WHEN DATE_DIFF(r.greg_end_dt, r.last_purchase_mcom, MONTH) <= 12 THEN 'RETAINED'
          WHEN DATE_DIFF(r.greg_end_dt, r.last_purchase_mcom, MONTH) BETWEEN 13 AND 24 THEN 'LAPSED'
          WHEN DATE_DIFF(r.greg_end_dt, r.last_purchase_mcom, MONTH) > 24 AND r.first_purchase_mcom IS NOT NULL THEN 'INACTIVE'
          ELSE NULL
        END,
        CASE
          WHEN r.first_purchase_store = r.greg_strt_dt THEN 'NET NEW'
          WHEN p.latest_purchase_in_period_store IS NOT NULL 
            AND LAG(r.last_purchase_store, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt) IS NOT NULL
            AND DATE_DIFF(p.latest_purchase_in_period_store, 
                          LAG(r.last_purchase_store, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt), 
                          MONTH) >= 12
            AND DATE_DIFF(p.latest_purchase_in_period_store, 
                          LAG(r.last_purchase_store, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt), 
                          MONTH) <= 24 THEN 'REACTIVE'
          WHEN p.latest_purchase_in_period_store IS NOT NULL 
            AND LAG(r.last_purchase_store, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt) IS NOT NULL
            AND DATE_DIFF(p.latest_purchase_in_period_store, 
                          LAG(r.last_purchase_store, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt), 
                          MONTH) >= 12 THEN 'NEW'
          WHEN DATE_DIFF(r.greg_end_dt, r.last_purchase_store, MONTH) <= 12 THEN 'RETAINED'
          WHEN DATE_DIFF(r.greg_end_dt, r.last_purchase_store, MONTH) BETWEEN 13 AND 24 THEN 'LAPSED'
          WHEN DATE_DIFF(r.greg_end_dt, r.last_purchase_store, MONTH) > 24 AND r.first_purchase_store IS NOT NULL THEN 'INACTIVE'
          ELSE NULL
        END
      ) IN ('NET NEW', 'NEW', 'RETAINED', 'REACTIVE') THEN 'Y'
      ELSE 'N'
    END AS application_flag,
    -- Flag description: Explain the flag value
    CASE
      WHEN COALESCE(
        CASE
          WHEN r.first_purchase = r.greg_strt_dt THEN 'NET NEW'
          WHEN p.latest_purchase_in_period_total IS NOT NULL 
            AND LAG(r.last_purchase, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt) IS NOT NULL
            AND DATE_DIFF(p.latest_purchase_in_period_total, 
                          LAG(r.last_purchase, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt), 
                          MONTH) >= 12
            AND DATE_DIFF(p.latest_purchase_in_period_total, 
                          LAG(r.last_purchase, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt), 
                          MONTH) <= 24 THEN 'REACTIVE'
          WHEN p.latest_purchase_in_period_total IS NOT NULL 
            AND LAG(r.last_purchase, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt) IS NOT NULL
            AND DATE_DIFF(p.latest_purchase_in_period_total, 
                          LAG(r.last_purchase, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt), 
                          MONTH) >= 12 THEN 'NEW'
          WHEN DATE_DIFF(r.greg_end_dt, r.last_purchase, MONTH) <= 12 THEN 'RETAINED'
          WHEN DATE_DIFF(r.greg_end_dt, r.last_purchase, MONTH) BETWEEN 13 AND 24 THEN 'LAPSED'
          WHEN DATE_DIFF(r.greg_end_dt, r.last_purchase, MONTH) > 24 AND r.first_purchase IS NOT NULL THEN 'INACTIVE'
          ELSE NULL
        END,
        CASE
          WHEN r.first_purchase_mcom = r.greg_strt_dt THEN 'NET NEW'
          WHEN p.latest_purchase_in_period_mcom IS NOT NULL 
            AND LAG(r.last_purchase_mcom, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt) IS NOT NULL
            AND DATE_DIFF(p.latest_purchase_in_period_mcom, 
                          LAG(r.last_purchase_mcom, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt), 
                          MONTH) >= 12
            AND DATE_DIFF(p.latest_purchase_in_period_mcom, 
                          LAG(r.last_purchase_mcom, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt), 
                          MONTH) <= 24 THEN 'REACTIVE'
          WHEN p.latest_purchase_in_period_mcom IS NOT NULL 
            AND LAG(r.last_purchase_mcom, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt) IS NOT NULL
            AND DATE_DIFF(p.latest_purchase_in_period_mcom, 
                          LAG(r.last_purchase_mcom, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt), 
                          MONTH) >= 12 THEN 'NEW'
          WHEN DATE_DIFF(r.greg_end_dt, r.last_purchase_mcom, MONTH) <= 12 THEN 'RETAINED'
          WHEN DATE_DIFF(r.greg_end_dt, r.last_purchase_mcom, MONTH) BETWEEN 13 AND 24 THEN 'LAPSED'
          WHEN DATE_DIFF(r.greg_end_dt, r.last_purchase_mcom, MONTH) > 24 AND r.first_purchase_mcom IS NOT NULL THEN 'INACTIVE'
          ELSE NULL
        END,
        CASE
          WHEN r.first_purchase_store = r.greg_strt_dt THEN 'NET NEW'
          WHEN p.latest_purchase_in_period_store IS NOT NULL 
            AND LAG(r.last_purchase_store, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt) IS NOT NULL
            AND DATE_DIFF(p.latest_purchase_in_period_store, 
                          LAG(r.last_purchase_store, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt), 
                          MONTH) >= 12
            AND DATE_DIFF(p.latest_purchase_in_period_store, 
                          LAG(r.last_purchase_store, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt), 
                          MONTH) <= 24 THEN 'REACTIVE'
          WHEN p.latest_purchase_in_period_store IS NOT NULL 
            AND LAG(r.last_purchase_store, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt) IS NOT NULL
            AND DATE_DIFF(p.latest_purchase_in_period_store, 
                          LAG(r.last_purchase_store, 1) OVER (PARTITION BY r.indiv_id ORDER BY r.greg_strt_dt), 
                          MONTH) >= 12 THEN 'NEW'
          WHEN DATE_DIFF(r.greg_end_dt, r.last_purchase_store, MONTH) <= 12 THEN 'RETAINED'
          WHEN DATE_DIFF(r.greg_end_dt, r.last_purchase_store, MONTH) BETWEEN 13 AND 24 THEN 'LAPSED'
          WHEN DATE_DIFF(r.greg_end_dt, r.last_purchase_store, MONTH) > 24 AND r.first_purchase_store IS NOT NULL THEN 'INACTIVE'
          ELSE NULL
        END
      ) IN ('NET NEW', 'NEW', 'RETAINED', 'REACTIVE') 
        THEN 'Eligible: Active customer (Net New, New, Retained, or Reactive)'
      ELSE 'Not Eligible: Inactive or Lapsed customer'
    END AS flag_description
  FROM roll r
  LEFT JOIN purchases_in_period p
    ON p.indiv_id = r.indiv_id
    AND p.greg_strt_dt = r.greg_strt_dt
    AND p.greg_end_dt = r.greg_end_dt
),

final_rows AS (
  -- Step 13: Final output with all required columns
  SELECT
    91 AS gmm_id,  -- Placeholder value as per sample output
    indiv_id AS INDV_ID,
    greg_strt_dt,
    greg_end_dt,
    'TOTAL' AS FILTER,
    'TOTAL' AS FILTER_LEVEL,
    total_cs AS TOTAL_CS,
    mcom_cs AS MCOM_CS,
    store_cs AS STORE_CS,
    last_purchase_dt_mcom,
    last_purchase_dt_store,
    application_flag AS APPLICATION_FLAG,
    flag_description AS FLAG_DESCRIPTION
  FROM labeled
)

-- Final output
SELECT
  gmm_id,
  INDV_ID,
  greg_strt_dt,
  greg_end_dt,
  FILTER,
  FILTER_LEVEL,
  TOTAL_CS,
  MCOM_CS,
  STORE_CS,
  last_purchase_dt_mcom,
  last_purchase_dt_store,
  APPLICATION_FLAG,
  FLAG_DESCRIPTION
FROM final_rows
ORDER BY INDV_ID, greg_strt_dt;


qury1:

-- Declare constants for lookback period and current date
DECLARE LOOKBACK_DAYS INT64 DEFAULT 2500;
DECLARE RUN_DATE DATE DEFAULT CURRENT_DATE;

-- Generate a continuous date range based on the lookback window
WITH date_range AS (
    SELECT DISTINCT d AS greg_date
    FROM UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(RUN_DATE, INTERVAL LOOKBACK_DAYS DAY), RUN_DATE)) AS d
),

-- Select distinct customer IDs with valid attributes
customer_dates AS (
    SELECT c.indiv_id, d.greg_date
    FROM (
        SELECT DISTINCT indiv_id
        FROM `mtech-daas-transact-sdata.rfnd_sls.merch_all`
        WHERE 
            DATE(txn_dt) >= DATE_SUB(RUN_DATE, INTERVAL LOOKBACK_DAYS DAY)
            AND prch_chnl_cd IN ('A','F')
            AND fin_own_lease_ind = 'Y'
            AND rtl_divn_nbr IN (71,77)
            AND pdiv_id NOT IN (000, 065)
            AND dept_vnd_cd IS NOT NULL
            AND indiv_id IS NOT NULL
            AND LENGTH(CAST(indiv_id AS STRING)) >= 8
            AND indiv_id != 1
            AND vst_cd IS NOT NULL
            AND indiv_id = 10018809
    ) c
    CROSS JOIN date_range d
),

-- Extract and prepare base transaction records
tx AS (
    SELECT
        indiv_id,
        DATE(txn_dt) AS tx_date,
        prch_chnl_cd,
        'TOTAL' AS total_key
    FROM `mtech-daas-transact-sdata.rfnd_sls.merch_all`
    WHERE 
        DATE(txn_dt) >= DATE_SUB(RUN_DATE, INTERVAL LOOKBACK_DAYS DAY)
        AND prch_chnl_cd IN ('A', 'F')
        AND fin_own_lease_ind = 'Y'
        AND rtl_divn_nbr IN (71, 77)
        AND pdiv_id NOT IN (000, 065)
        AND dept_vnd_cd IS NOT NULL
        AND indiv_id IS NOT NULL
        AND LENGTH(CAST(indiv_id AS STRING)) >= 8
        AND indiv_id != 1
        AND vst_cd IS NOT NULL
        AND indiv_id = 10018809
),

-- Compute transaction dates and gaps
identified_txn_dates AS (
    SELECT 
        indiv_id, 
        LAG(tx_date) OVER w AS prv_tx_date,
        tx_date,
        COALESCE(LEAD(tx_date) OVER w, DATE '3499-12-31') AS nxt_tx_dt,
        MIN(tx_date) OVER (PARTITION BY indiv_id) AS first_purchase,
        MAX(tx_date) OVER (PARTITION BY indiv_id) AS last_purchase,
        DATE_DIFF(LEAD(tx_date) OVER w, tx_date, MONTH) AS months_between_txn_nxt,
        DATE_DIFF(tx_date, LAG(tx_date) OVER w, MONTH) AS months_between_txn_prv
    FROM tx
    WINDOW w AS (PARTITION BY indiv_id ORDER BY tx_date)
),

-- Roll-up transaction information per customer-date
roll AS (
    SELECT
        cd.indiv_id,
        cd.greg_date,
        tx.prv_tx_date,
        tx.tx_date,
        tx.nxt_tx_dt,
        tx.first_purchase,
        tx.last_purchase,
        tx.months_between_txn_nxt,
        tx.months_between_txn_prv
    FROM customer_dates cd
    LEFT JOIN identified_txn_dates tx 
        ON tx.indiv_id = cd.indiv_id AND cd.greg_date BETWEEN tx.tx_date AND tx.nxt_tx_dt
    WHERE cd.greg_date >= tx.first_purchase
),

-- Assign Lifecycle Labels
labeled AS (
    SELECT
        indiv_id, greg_date, 
        tx_date,
        months_between_txn_prv,
        CASE
            WHEN first_purchase = greg_date THEN 'NET NEW'
            WHEN months_between_txn_prv >= 24 AND tx_date = greg_date THEN 'NEW'
            WHEN months_between_txn_prv BETWEEN 13 AND 24 AND tx_date = greg_date THEN 'REACTIVE'
            WHEN DATE_DIFF(greg_date, tx_date, MONTH) BETWEEN 13 AND 24 AND tx_date <> greg_date THEN 'LAPSED'
            WHEN DATE_DIFF(greg_date, tx_date, MONTH) > 24 THEN 'INACTIVE'
            WHEN DATE_DIFF(greg_date, tx_date, MONTH) <= 12 THEN 'RETAINED'
        END AS customer_state
    FROM roll
    WHERE greg_date BETWEEN tx_date AND nxt_tx_dt
),

-- Final filter for recent dates
yearly_data AS (
    SELECT 
        greg_date, indiv_id,
        customer_state,
        0 as Attr_Flag,
        'Is not WBR Report Hierarchy' as Flag_Desc
    FROM labeled
    WHERE DATE_DIFF(RUN_DATE, greg_date, DAY) <= 2500
)

-- Final output
SELECT 
    MIN(greg_date) AS GREG_START_DT,
    MAX(greg_date) AS GREG_END_DT,
    indiv_id,
    customer_state,
    Attr_Flag,
    Flag_Desc
FROM yearly_data
GROUP BY indiv_id, customer_state, Attr_Flag, Flag_Desc
ORDER BY indiv_id, GREG_START_DT;


----
CREATE OR REPLACE TABLE `project.dataset.customer_lifecycle_final` AS
SELECT 
    MIN(greg_date) AS GREG_START_DT,
    MAX(greg_date) AS GREG_END_DT,
    indiv_id,
    customer_state,
    0 as Attr_Flag,
    'Is not WBR Report Hierarchy' as Flag_Desc
FROM yearly_data
GROUP BY indiv_id, customer_state, Attr_Flag, Flag_Desc;


CREATE OR REPLACE PROCEDURE `project.dataset.sp_customer_lifecycle_incremental`()
BEGIN

  -- Step 1: Create a temporary table with new/updated results
  CREATE OR REPLACE TEMP TABLE recent_lifecycle_updates AS
  WITH date_range AS (
      SELECT DISTINCT d AS greg_date
      FROM UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY), CURRENT_DATE())) AS d
  ),
  customer_dates AS (
      SELECT c.indiv_id, d.greg_date
      FROM (
          SELECT DISTINCT indiv_id
          FROM `mtech-daas-transact-sdata.rfnd_sls.merch_all`
          WHERE 
              DATE(txn_dt) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2500 DAY)
              AND prch_chnl_cd IN ('A','F')
              AND fin_own_lease_ind = 'Y'
              AND rtl_divn_nbr IN (71,77)
              AND pdiv_id NOT IN (000, 065)
              AND dept_vnd_cd IS NOT NULL
              AND indiv_id IS NOT NULL
              AND LENGTH(CAST(indiv_id AS STRING)) >= 8
              AND indiv_id != 1
              AND vst_cd IS NOT NULL
      ) c
      CROSS JOIN date_range d
  ),
  tx AS (
      SELECT
          indiv_id,
          DATE(txn_dt) AS tx_date,
          prch_chnl_cd,
          'TOTAL' AS total_key
      FROM `mtech-daas-transact-sdata.rfnd_sls.merch_all`
      WHERE 
          DATE(txn_dt) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2500 DAY)
          AND prch_chnl_cd IN ('A', 'F')
          AND fin_own_lease_ind = 'Y'
          AND rtl_divn_nbr IN (71, 77)
          AND pdiv_id NOT IN (000, 065)
          AND dept_vnd_cd IS NOT NULL
          AND indiv_id IS NOT NULL
          AND LENGTH(CAST(indiv_id AS STRING)) >= 8
          AND indiv_id != 1
          AND vst_cd IS NOT NULL
  ),
  identified_txn_dates AS (
      SELECT 
          indiv_id, 
          LAG(tx_date) OVER w AS prv_tx_date,
          tx_date,
          COALESCE(LEAD(tx_date) OVER w, DATE '3499-12-31') AS nxt_tx_dt,
          MIN(tx_date) OVER (PARTITION BY indiv_id) AS first_purchase,
          MAX(tx_date) OVER (PARTITION BY indiv_id) AS last_purchase,
          DATE_DIFF(LEAD(tx_date) OVER w, tx_date, MONTH) AS months_between_txn_nxt,
          DATE_DIFF(tx_date, LAG(tx_date) OVER w, MONTH) AS months_between_txn_prv
      FROM tx
      WINDOW w AS (PARTITION BY indiv_id ORDER BY tx_date)
  ),
  roll AS (
      SELECT
          cd.indiv_id,
          cd.greg_date,
          tx.prv_tx_date,
          tx.tx_date,
          tx.nxt_tx_dt,
          tx.first_purchase,
          tx.last_purchase,
          tx.months_between_txn_nxt,
          tx.months_between_txn_prv
      FROM customer_dates cd
      LEFT JOIN identified_txn_dates tx 
          ON tx.indiv_id = cd.indiv_id AND cd.greg_date BETWEEN tx.tx_date AND tx.nxt_tx_dt
      WHERE cd.greg_date >= tx.first_purchase
  ),
  labeled AS (
      SELECT
          indiv_id, greg_date, 
          tx_date,
          months_between_txn_prv,
          CASE
              WHEN first_purchase = greg_date THEN 'NET NEW'
              WHEN months_between_txn_prv >= 24 AND tx_date = greg_date THEN 'NEW'
              WHEN months_between_txn_prv BETWEEN 13 AND 24 AND tx_date = greg_date THEN 'REACTIVE'
              WHEN DATE_DIFF(greg_date, tx_date, MONTH) BETWEEN 13 AND 24 AND tx_date <> greg_date THEN 'LAPSED'
              WHEN DATE_DIFF(greg_date, tx_date, MONTH) > 24 THEN 'INACTIVE'
              WHEN DATE_DIFF(greg_date, tx_date, MONTH) <= 12 THEN 'RETAINED'
          END AS customer_state
      FROM roll
      WHERE greg_date BETWEEN tx_date AND nxt_tx_dt
  )
  SELECT 
      MIN(greg_date) AS GREG_START_DT,
      MAX(greg_date) AS GREG_END_DT,
      indiv_id,
      customer_state,
      0 as Attr_Flag,
      'Is not WBR Report Hierarchy' as Flag_Desc
  FROM labeled
  GROUP BY indiv_id, customer_state, Attr_Flag, Flag_Desc;

  -- Step 2: Merge into main table (Upsert logic)
  MERGE `project.dataset.customer_lifecycle_final` AS target
  USING recent_lifecycle_updates AS source
  ON target.indiv_id = source.indiv_id AND target.customer_state = source.customer_state
  WHEN MATCHED AND (target.GREG_END_DT <> source.GREG_END_DT OR target.GREG_START_DT <> source.GREG_START_DT) THEN
    UPDATE SET 
        target.GREG_START_DT = source.GREG_START_DT,
        target.GREG_END_DT = source.GREG_END_DT
  WHEN NOT MATCHED THEN
    INSERT (GREG_START_DT, GREG_END_DT, indiv_id, customer_state, Attr_Flag, Flag_Desc)
    VALUES (source.GREG_START_DT, source.GREG_END_DT, source.indiv_id, source.customer_state, source.Attr_Flag, source.Flag_Desc);

END;


-- One-time full load
CALL `project.dataset.sp_customer_lifecycle_incremental`();
