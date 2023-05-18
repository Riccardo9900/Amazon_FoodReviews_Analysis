-- Creazione della tabella per i dati delle recensioni
CREATE TABLE IF NOT EXISTS recensioni (
  Id STRING,
  ProductId STRING,
  UserId STRING,
  ProfileName STRING,
  HelpfulnessNumerator INT,
  HelpfulnessDenominator INT,
  Score INT,
  Time BIGINT,
  Summary STRING,
  Text STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

-- Caricamento dei dati nel formato CSV nella tabella reviews
LOAD DATA LOCAL INPATH 'PycharmProjects/job1/Reviews.csv' OVERWRITE INTO TABLE recensioni;

DROP VIEW IF EXISTS recensioni_view;

CREATE VIEW recensioni_view AS
SELECT *, YEAR(FROM_UNIXTIME(Time)) AS Year
FROM recensioni;

WITH top_products AS (
  SELECT Year, ProductId, ReviewCount
  FROM (
    SELECT Year, ProductId, COUNT(*) AS ReviewCount, ROW_NUMBER() OVER (PARTITION BY Year ORDER BY COUNT(*) DESC) AS rn
    FROM recensioni_view
    GROUP BY Year, ProductId
  ) tmp
  WHERE rn <= 10 
),
top_words AS (
  SELECT t.Year, t.ProductId, t.ReviewCount, Word, COUNT(*) AS WordCount
  FROM top_products t
  JOIN
    (
      SELECT Year, ProductId, Word
      FROM recensioni_view 
      LATERAL VIEW explode(split(lower(Text), ' ')) temp AS Word
      WHERE LENGTH(Word) >= 4
    ) temp ON t.Year = temp.Year AND t.ProductId = temp.ProductId
  GROUP BY t.Year, t.ProductId, t.ReviewCount, Word
),
ranked_words AS (
  SELECT Year, ProductId, ReviewCount, Word, WordCount, ROW_NUMBER() OVER (PARTITION BY Year, ProductId, ReviewCount ORDER BY WordCount DESC) AS rn
  FROM top_words
)
SELECT Year, ProductId, ReviewCount, Word, WordCount
FROM (
  SELECT Year, ProductId, ReviewCount, Word, WordCount, ROW_NUMBER() OVER (PARTITION BY Year, ProductId ORDER BY WordCount DESC) AS word_rn
  FROM ranked_words
) ranked_words_with_rownum
WHERE word_rn <= 5
ORDER BY Year, ProductId, ReviewCount DESC, WordCount DESC;
