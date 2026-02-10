SELECT * FROM `IMDB.internet_usage` LIMIT 5;

/* Business questions */
-- Weekly avg screen time

-- How many WiFi vs Mobile data users
SELECT
  internet_type,
  COUNT(user_id) AS users,
  SUM(COUNT(user_id)) OVER () AS total_users,
  round((COUNT(user_id) / SUM(COUNT(user_id)) OVER () * 100), 1)
    AS percentage_split
FROM `IMDB.internet_usage`
GROUP BY 1;

-- How many phone vs tablet users
SELECT
  primary_device,
  COUNT(user_id),
  sum(COUNT(user_id)) OVER () AS total_users,
  round((COUNT(user_id) / SUM(COUNT(user_id)) OVER () * 100), 1)
FROM `IMDB.internet_usage`
GROUP BY 1
ORDER BY 4 DESC;

-- Whos is wasting more time on which device (total_screen_time)
SELECT primary_device, highest_screen_time, user_id, age_group
FROM
  (
    SELECT
      primary_device,
      total_screen_time AS highest_screen_time,
      user_id,
      age_group,
      row_number()
        OVER (PARTITION BY primary_device ORDER BY total_screen_time DESC)
        AS highest_hours
    FROM `IMDB.internet_usage`
  )
WHERE highest_hours = 1;

-- In each Age group who has the highest total screen time
SELECT age_group, total_screen_time AS highest_screen_time, user_id
FROM
  (
    SELECT
      user_id,
      age_group,
      total_screen_time,
      row_number()
        OVER (PARTITION BY age_group ORDER BY total_screen_time DESC) AS rw
    FROM `IMDB.internet_usage`
  )
WHERE rw = 1
ORDER BY 1;

-- Top 10 highest screen time users in each age group (assuming that we are not taking users with same total screen time)
SELECT age_group, total_screen_time AS highest_screen_time, user_id
FROM
  (
    SELECT
      user_id,
      age_group,
      total_screen_time,
      row_number()
        OVER (PARTITION BY age_group ORDER BY total_screen_time DESC) AS rw
    FROM `IMDB.internet_usage`
  )
WHERE rw < 11
ORDER BY 1;

-- Finding out which age groups in each months have high total screen times across the year
WITH
  total_usage_hours AS (
    SELECT
      EXTRACT(month FROM date) AS usage_month,
      age_group,
      round(SUM(total_screen_time), 1) AS screentime_over_year
    FROM `IMDB.internet_usage`
    GROUP BY 1, 2
    ORDER BY 1, 2
  ),
  ranking AS (
    SELECT
      usage_month,
      age_group,
      screentime_over_year,
      rank()
        OVER (PARTITION BY usage_month ORDER BY screentime_over_year DESC) AS rw
    FROM total_usage_hours
  )
SELECT usage_month, age_group, screentime_over_year
FROM ranking
WHERE rw = 1
ORDER BY 1;

---- Month trend for hours ; social, work and entertainment

