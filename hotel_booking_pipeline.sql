
CREATE OR REFRESH STREAMING TABLE bronze_hotel_bookings
AS
SELECT
    *,
    _metadata.file_name AS source_file,
    current_timestamp() AS loaded_at
FROM STREAM read_files(
    '/Volumes/hospitality_catalog/bookings/raw_files/',
    format => 'csv',
    header => true
);


-- STEP 04: SILVER LAYER
-- Remove duplicate bookings

CREATE OR REFRESH MATERIALIZED VIEW
silver_bookings_deduplicated AS

SELECT * EXCEPT (row_num)
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY booking_id
            ORDER BY booking_id
        ) AS row_num
    FROM bronze_hotel_bookings
)
WHERE row_num = 1;


-- STEP 04: SILVER LAYER - CLEAN BOOKINGS

CREATE OR REFRESH MATERIALIZED VIEW silver_bookings_clean (
    CONSTRAINT valid_stay_dates
        EXPECT (check_out_date > check_in_date)
        ON VIOLATION DROP ROW,

    CONSTRAINT valid_room_rate
        EXPECT (room_rate >= 0)
        ON VIOLATION DROP ROW
)
AS

WITH cleaned AS (
    SELECT
        TRIM(booking_id) AS booking_id,

        NULLIF(TRIM(guest_id), '') AS guest_id,

        TRY_CAST(booking_date AS DATE) AS booking_date,

        TRY_CAST(check_in_date AS DATE) AS check_in_date,

        TRY_CAST(check_out_date AS DATE) AS check_out_date,

        COALESCE(
            TRY_CAST(NULLIF(TRIM(nights), '') AS INT),
            DATEDIFF(check_out_date, check_in_date)
        ) AS nights,

        TRIM(hotel_name) AS hotel_name,

        COALESCE(
            INITCAP(NULLIF(TRIM(city), '')),
            'Unknown'
        ) AS city,

        INITCAP(TRIM(region)) AS region,

        INITCAP(TRIM(room_type)) AS room_type,

        COALESCE(
            INITCAP(NULLIF(TRIM(booking_channel), '')),
            'Unknown'
        ) AS booking_channel,

        TRY_CAST(guests AS INT) AS guests,

        TRY_CAST(room_rate AS DOUBLE) AS room_rate,

        TRY_CAST(total_amount AS DOUBLE) AS total_amount,

        COALESCE(
            TRY_CAST(discount_amount AS DOUBLE),
            0
        ) AS discount_amount,

        INITCAP(TRIM(meal_plan)) AS meal_plan,

        TRIM(payment_method) AS payment_method,

        TRIM(payment_status) AS payment_status,

        INITCAP(TRIM(booking_status)) AS booking_status,

        TRY_CAST(
            NULLIF(TRIM(guest_rating), '') AS INT
        ) AS guest_rating

    FROM silver_bookings_deduplicated
)

SELECT
    * EXCEPT (total_amount),

    COALESCE(
        total_amount,
        nights * room_rate
    ) AS total_amount

FROM cleaned;


-- STEP 04: SILVER REJECTED BOOKINGS

CREATE OR REFRESH MATERIALIZED VIEW
silver_bookings_rejected AS

SELECT *,
    CASE
        WHEN TRY_CAST(check_out_date AS DATE)
             <= TRY_CAST(check_in_date AS DATE)
        THEN 'Invalid stay dates'

        WHEN TRY_CAST(room_rate AS DOUBLE) < 0
        THEN 'Negative room rate'

    END AS reject_reason

FROM silver_bookings_deduplicated

WHERE
    TRY_CAST(check_out_date AS DATE)
    <= TRY_CAST(check_in_date AS DATE)

    OR TRY_CAST(room_rate AS DOUBLE) < 0;


-- STEP 05: GOLD LAYER - MONTHLY BOOKINGS SUMMARY

CREATE OR REFRESH MATERIALIZED VIEW gold_monthly_bookings AS
SELECT
    DATE_TRUNC('MONTH', check_in_date) AS stay_month,
    COUNT(*) AS bookings,
    COUNT_IF(booking_status = 'Cancelled') AS cancelled_bookings,
    SUM(
        CASE WHEN booking_status = 'Completed'
             THEN nights
        END
    ) AS room_nights,
    SUM(
        CASE WHEN booking_status = 'Completed'
             THEN total_amount - discount_amount
        END
    ) AS net_revenue
FROM silver_bookings_clean
GROUP BY DATE_TRUNC('MONTH', check_in_date);


-- STEP 06: GOLD LAYER - HOTEL SUMMARY

CREATE OR REFRESH MATERIALIZED VIEW gold_hotel_summary AS
SELECT
    region,
    city,
    hotel_name,

    -- All bookings (Completed and Cancelled)
    COUNT(*) AS bookings,

    -- Completed bookings only
    SUM(
        CASE WHEN booking_status = 'Completed'
             THEN nights
        END
    ) AS room_nights,

    SUM(
        CASE WHEN booking_status = 'Completed'
             THEN total_amount - discount_amount
        END
    ) AS net_revenue,

    -- Average daily rate: net_revenue / room_nights, NULL-guarded
    SUM(
        CASE WHEN booking_status = 'Completed'
             THEN total_amount - discount_amount
        END
    ) / NULLIF(
        SUM(
            CASE WHEN booking_status = 'Completed'
                 THEN nights
            END
        ), 0
    ) AS avg_daily_rate

FROM silver_bookings_clean
GROUP BY region, city, hotel_name;


-- STEP 07: GOLD LAYER - ROOM TYPE SUMMARY

CREATE OR REFRESH MATERIALIZED VIEW gold_room_type_summary AS
SELECT
    room_type,

    -- All bookings (Completed and Cancelled)
    COUNT(*) AS bookings,

    -- Completed bookings only
    COALESCE(
        SUM(
            CASE WHEN booking_status = 'Completed'
                 THEN nights
            END
        ), 0
    ) AS room_nights,

    COALESCE(
        SUM(
            CASE WHEN booking_status = 'Completed'
                 THEN total_amount - discount_amount
            END
        ), 0
    ) AS net_revenue,

    -- Completed bookings only; AVG ignores NULL guest_rating values
    AVG(
        CASE WHEN booking_status = 'Completed'
             THEN guest_rating
        END
    ) AS avg_rating

FROM silver_bookings_clean
GROUP BY room_type;


-- STEP 08: GOLD LAYER - BOOKING CHANNEL SUMMARY

CREATE OR REFRESH MATERIALIZED VIEW gold_channel_summary AS
SELECT
    booking_channel,

    -- All bookings (Completed and Cancelled)
    COUNT(*) AS bookings,

    COUNT_IF(booking_status = 'Cancelled') AS cancelled_bookings,

    -- Cancelled share of all bookings, NULL-guarded against division by zero
    COUNT_IF(booking_status = 'Cancelled') * 100.0
        / NULLIF(COUNT(*), 0) AS cancellation_rate_pct,

    -- Completed bookings only; Cancelled contribute zero revenue
    SUM(
        CASE WHEN booking_status = 'Completed'
             THEN total_amount - discount_amount
             ELSE 0
        END
    ) AS net_revenue

FROM silver_bookings_clean
GROUP BY booking_channel;


-- STEP 09: GOLD LAYER - DATA QUALITY SUMMARY

-- Row counts across layers, plus deduplicated rejection count.
-- Raw data issue counts are measured on silver_bookings_deduplicated
-- (raw values, after duplicate removal, before cleaning) so they are
-- independent of the cleaning step and of the dedup rejection count.
CREATE OR REFRESH MATERIALIZED VIEW gold_data_quality AS
WITH counts AS (
    SELECT
        (SELECT COUNT(*) FROM bronze_hotel_bookings)
            AS bronze_rows,
        (SELECT COUNT(*) FROM silver_bookings_deduplicated)
            AS deduplicated_rows,
        (SELECT COUNT(*) FROM silver_bookings_clean)
            AS clean_rows,
        (SELECT COUNT(*) FROM silver_bookings_rejected)
            AS rejected_rows
),
raw_issues AS (
    SELECT
        COUNT_IF(
            TRY_CAST(check_out_date AS DATE)
                <= TRY_CAST(check_in_date AS DATE)
        ) AS invalid_date_rows,

        COUNT_IF(TRY_CAST(room_rate AS DOUBLE) < 0)
            AS negative_rate_rows,

        COUNT_IF(city IS NULL OR TRIM(city) = '')
            AS missing_city_rows,

        COUNT_IF(booking_channel IS NULL OR TRIM(booking_channel) = '')
            AS missing_channel_rows,

        COUNT_IF(nights IS NULL OR TRIM(nights) = '')
            AS missing_nights_rows,

        COUNT_IF(total_amount IS NULL OR TRIM(total_amount) = '')
            AS missing_amount_rows
    FROM silver_bookings_deduplicated
)
SELECT
    -- Row counts across layers
    counts.bronze_rows,

    -- Deduplicated rejection count: Bronze rows removed by dedup
    counts.bronze_rows - counts.deduplicated_rows
        AS duplicate_rows,

    counts.clean_rows,
    counts.rejected_rows,

    -- Raw data issue counts (from deduplicated, pre-clean data)
    raw_issues.invalid_date_rows,
    raw_issues.negative_rate_rows,
    raw_issues.missing_city_rows,
    raw_issues.missing_channel_rows,
    raw_issues.missing_nights_rows,
    raw_issues.missing_amount_rows,

    -- Reconciliation: Bronze should equal Clean + Rejected + Duplicates
    counts.bronze_rows = (
        counts.clean_rows
        + counts.rejected_rows
        + (counts.bronze_rows - counts.deduplicated_rows)
    ) AS reconciliation_passed
FROM counts
CROSS JOIN raw_issues;

