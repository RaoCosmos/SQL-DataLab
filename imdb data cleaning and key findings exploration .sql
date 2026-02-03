-- View Messy Data
SELECT * FROM `IMDB.movies` ORDER BY 1 ASC LIMIT 100;

-- Raw Data Ingestion
-- remove duplicates; there are a lot of duplicates
-- create staging for raw without duplicates
CREATE OR REPLACE TABLE `IMDB.movies_raw `
AS (
  WITH
    no_duplicate_records AS (
      SELECT
        movies,
        year,
        genre,
        rating,
        `one-line`,
        stars,
        votes,
        runtime,
        gross,
        row_number() OVER (PARTITION BY movies) AS rn
      FROM `IMDB.movies`
    )
  SELECT movies, year, genre, rating, `one-line`, stars, votes, runtime, gross
  FROM no_duplicate_records
  WHERE rn = 1
);

-- Staging and Transformation layer
-- Purpose: standardize data types and normalize text fields for downstream cleaning
CREATE OR REPLACE TABLE `IMDB.movies_stage`
AS
SELECT
  UPPER(movies) AS Movie_name,
  SAFE_CAST(REGEXP_EXTRACT(YEAR, r'\d{4}') AS INT64) AS Release_year,
  regexp_extract(Genre, r'([a-zA-Z]+)') AS Category,
  rating,
  `one-line`,
  stars,
  votes,
  runtime,
  SAFE_CAST(REPLACE(REPLACE(Gross, '$', ''), 'M', '') AS FLOAT64)
    AS boxoffice_collection
FROM `IMDB.movies_raw `;

-- Null value profiling
-- Purpose: assess data completeness and identify critical columns with missing values
SELECT
  COUNT(*) AS total,
  COUNTIF(release_year IS NULL) AS totalnulls_in_release_year,
  COUNTIF(Movie_Name IS NULL) AS totalnulls_in_movies,
  COUNTIF(Category IS NULL) AS totalnulls_in_genre,
  COUNTIF(rating IS NULL) AS totalnulls_in_rating,
  COUNTIF(`one-line` IS NULL) AS totalnulls_in_one_line,
  COUNTIF(stars IS NULL) AS totalnulls_in_stars,
  COUNTIF(votes IS NULL) AS totalnulls_in_votes,
  COUNTIF(Runtime IS NULL) AS totalnulls_in_runtime,
  COUNTIF(boxoffice_collection IS NULL) AS totalnulls_in_gross
FROM `IMDB.movies_stage`;

-- Final clean table for analytics
-- Purpose: retain validated, deduplicated,and business-ready columns
CREATE OR REPLACE TABLE `IMDB.movies_clean`
AS
SELECT
  Movie_Name,
  Release_year,
  ifnull(Category, 'Unknown') AS Genre,
  rating,
  ifnull(stars, 'Unknown') AS Casting,
  votes,
  runtime,
  boxoffice_Collection
FROM `IMDB.movies_stage`;

SELECT * FROM `IMDB.movies_clean`;

-- Analysis layer
-- Purpose: derive insights from the finalized clean movies dataset

-- Top 5 highest Grossing years
SELECT release_year, round(sum(boxoffice_collection), 1) AS total
FROM `IMDB.movies_clean`
GROUP BY 1
ORDER BY 2 DESC;
/* Key findings
1. 2016 - 2.2 Billion $
2. 2017 - 2.1 Billion $
3. 2013 - 2 Billion $
4. 2012 - 1.2 Billion $
5. 2002 - 9.5 Million $ */

-- Top 5 Genres based on rating
SELECT genre, max(rating) FROM `IMDB.movies_clean` GROUP BY 1 ORDER BY 2 DESC;
/* Key findings
1. Crime - 9.4
2. Talk - 9.4
3. Documentary - 9.3
4. Animation - 9.2
5. Comedy - 9.2 */

-- Longest Avg Runtime by Genre
SELECT genre, avg(runtime) FROM `IMDB.movies_clean` GROUP BY 1 ORDER BY 2 DESC;
/* Key findings
1. News Genre has the highest avg running time at 156 minutes - 2.6hrs
2. History Genre at 118 minutes - 1.9hrs
3. Film Genres at 110 minutes - under 2 hours */

-- Vote Split
SELECT genre, sum(votes) FROM `IMDB.movies_clean` GROUP BY 1 ORDER BY 2 DESC;
/* Key findings
1. Action - 40 million votes
2. Comedy - 19 million votes
3. Drama - 18 million votes
4. Crime - 15 million votes
5. Animation - 12 million votes */

-- Most popular movie
WITH
  highest_vote AS (
    SELECT Movie_name, votes FROM `IMDB.movies_clean` ORDER BY 2 DESC LIMIT 5
  )
SELECT * FROM highest_vote LIMIT 5;
/* Key findings
Most Popular Movies by choice of viewer votes - Lord of the Rings at 17+ million votes, Breaking bad at 16.9 and The departed at 12.2 */

/* The end */
