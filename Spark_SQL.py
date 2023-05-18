from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode, year
from pyspark.sql.types import IntegerType, StringType

# Creazione della sessione Spark
spark = SparkSession.builder.appName("Recensioni").getOrCreate()

# Lettura del dataset CSV
df = spark.read.csv("/content/drive/MyDrive/Reviews_corretto.csv", header=True)

# Conversione dei tipi di dati appropriati
df = df.withColumn("HelpfulnessNumerator", df["HelpfulnessNumerator"].cast(IntegerType()))
df = df.withColumn("HelpfulnessDenominator", df["HelpfulnessDenominator"].cast(IntegerType()))
df = df.withColumn("Score", df["Score"].cast(IntegerType()))
df = df.withColumn("Time", df["Time"].cast(IntegerType()))
df = df.withColumn("Summary", df["Summary"].cast(StringType()))
df = df.withColumn("Text", df["Text"].cast(StringType()))

# Creazione di una vista temporanea per eseguire query SQL
df.createOrReplaceTempView("recensioni")

# Query per ottenere i 10 prodotti con il maggior numero di recensioni per ciascun anno
top_products_query = """
    SELECT Year, ProductId, ReviewCount
    FROM (
      SELECT YEAR(FROM_UNIXTIME(Time)) AS Year, ProductId, COUNT(*) AS ReviewCount,
      ROW_NUMBER() OVER (PARTITION BY YEAR(FROM_UNIXTIME(Time)) ORDER BY COUNT(*) DESC) AS rn
      FROM recensioni
      GROUP BY Year, ProductId
    ) tmp
    WHERE rn <= 10
    ORDER BY Year, ReviewCount DESC
    """

top_products_df = spark.sql(top_products_query)
top_products_df.createOrReplaceTempView("top_products")

# Query per ottenere le 5 parole più frequenti per ciascun prodotto
top_words_query = """
    SELECT t.Year, t.ProductId, t.ReviewCount, w.Word, w.WordCount
    FROM (
        SELECT Year, ProductId, ReviewCount
        FROM top_products
        WHERE (Year, ReviewCount) IN (
            SELECT Year, MAX(ReviewCount)
            FROM top_products
            GROUP BY Year
            )
        ) t
    JOIN (
        SELECT Year, ProductId, Word, WordCount
        FROM (
            SELECT Year, ProductId, word, COUNT(*) AS WordCount,
                ROW_NUMBER() OVER (PARTITION BY Year, ProductId ORDER BY COUNT(*) DESC) AS rank
            FROM (
                SELECT YEAR(FROM_UNIXTIME(Time)) AS Year, ProductId, EXPLODE(SPLIT(LOWER(Text), ' ')) AS word
                FROM recensioni
                )
            WHERE LENGTH(word) >= 4
            GROUP BY Year, ProductId, word
            )
        WHERE rank <= 5
        ) w
    ON t.Year = w.Year AND t.ProductId = w.ProductId
    ORDER BY t.Year, t.ReviewCount DESC, t.ProductId, w.WordCount DESC
    """

top_words_df = spark.sql(top_words_query)

# Visualizzazione dei risultati
top_words_df.show(truncate=False)
