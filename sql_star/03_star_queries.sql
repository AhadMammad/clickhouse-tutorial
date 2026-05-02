-- =============================================================================
-- ClickHouse Star Schema — Example Analytical Queries
-- Mobile App Interaction Analytics
-- =============================================================================

-- Daily Active Users (DAU) by platform
SELECT
    dd.full_date,
    dav.platform,
    uniq(f.user_key) AS daily_active_users
FROM fact_app_interactions f
JOIN dim_date dd ON f.date_key = dd.date_key
JOIN dim_app_version dav ON f.version_key = dav.version_key
WHERE dd.full_date >= today() - 30
GROUP BY dd.full_date, dav.platform
ORDER BY dd.full_date, dav.platform;

-- Top 10 screens by average session duration
SELECT
    ds.screen_name,
    ds.screen_category,
    count() AS total_events,
    avg(f.duration_ms) AS avg_event_duration_ms,
    avg(dim_s.duration_seconds) AS avg_session_duration_sec
FROM fact_app_interactions f
JOIN dim_screen ds ON f.screen_key = ds.screen_key
JOIN dim_session dim_s ON f.session_key = dim_s.session_key
WHERE f.event_timestamp >= now() - INTERVAL 30 DAY
GROUP BY ds.screen_name, ds.screen_category
ORDER BY total_events DESC
LIMIT 10;

-- Purchase funnel conversion rates
SELECT
    det.event_name,
    count() AS event_count,
    round(count() * 100.0 / max(count()) OVER (), 2) AS pct_of_top
FROM fact_app_interactions f
JOIN dim_event_type det ON f.event_type_key = det.event_type_key
WHERE det.event_name IN ('product_view', 'add_to_cart', 'checkout_start', 'purchase_complete')
  AND f.event_timestamp >= now() - INTERVAL 30 DAY
GROUP BY det.event_name
ORDER BY event_count DESC;

-- Event category distribution by OS
SELECT
    dd_dev.os_name,
    det.event_category,
    count() AS event_count,
    round(count() * 100.0 / sum(count()) OVER (PARTITION BY dd_dev.os_name), 2) AS pct
FROM fact_app_interactions f
JOIN dim_device dd_dev ON f.device_key = dd_dev.device_key
JOIN dim_event_type det ON f.event_type_key = det.event_type_key
WHERE f.event_timestamp >= now() - INTERVAL 7 DAY
GROUP BY dd_dev.os_name, det.event_category
ORDER BY dd_dev.os_name, event_count DESC;

-- Weekly Retention: users who return week after first session
SELECT
    toStartOfWeek(first_week.first_session) AS cohort_week,
    dateDiff('week', first_week.first_session, f.event_timestamp) AS weeks_since_first,
    uniq(f.user_key) AS retained_users
FROM fact_app_interactions f
JOIN (
    SELECT user_key, min(event_timestamp) AS first_session
    FROM fact_app_interactions
    GROUP BY user_key
) first_week ON f.user_key = first_week.user_key
WHERE f.event_timestamp >= now() - INTERVAL 90 DAY
GROUP BY cohort_week, weeks_since_first
ORDER BY cohort_week, weeks_since_first;

-- App version adoption over time
SELECT
    dd.full_date,
    dav.version_code,
    dav.platform,
    uniq(f.user_key) AS active_users
FROM fact_app_interactions f
JOIN dim_date dd ON f.date_key = dd.date_key
JOIN dim_app_version dav ON f.version_key = dav.version_key
WHERE dd.full_date >= today() - 14
GROUP BY dd.full_date, dav.version_code, dav.platform
ORDER BY dd.full_date, active_users DESC;

-- User tier engagement comparison
SELECT
    dut.tier_name,
    uniq(f.user_key) AS unique_users,
    count() AS total_events,
    avg(count()) OVER (PARTITION BY dut.tier_name) AS avg_events_per_user,
    avg(dim_s.duration_seconds) AS avg_session_duration_sec
FROM fact_app_interactions f
JOIN dim_user du ON f.user_key = du.user_key
JOIN dim_user_tier dut ON du.tier_key = dut.tier_key
JOIN dim_session dim_s ON f.session_key = dim_s.session_key
WHERE f.event_timestamp >= now() - INTERVAL 30 DAY
GROUP BY dut.tier_name, f.user_key, dim_s.duration_seconds
ORDER BY dut.tier_name;

-- Country-level session activity heatmap (hour of day × day of week)
SELECT
    f.country_code,
    toHour(f.event_timestamp) AS hour_of_day,
    toDayOfWeek(f.event_timestamp) AS day_of_week,
    uniq(f.session_key) AS sessions
FROM fact_app_interactions f
WHERE f.event_timestamp >= now() - INTERVAL 30 DAY
GROUP BY f.country_code, hour_of_day, day_of_week
ORDER BY f.country_code, day_of_week, hour_of_day;
