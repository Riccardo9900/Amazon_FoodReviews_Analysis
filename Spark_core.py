import re
from collections import Counter
from pyspark import SparkContext

# Crea il contesto Spark
sc = SparkContext.getOrCreate()

# Carica il file CSV come RDD
data = sc.textFile("/content/drive/MyDrive/Reviews_corretto.csv")

def get_year(timestamp):
  try:
    timestamp = int(timestamp)
    year = timestamp // 31536000
    return str(year)
  except ValueError:
    return "Anno sconosciuto"
  

def extract_words(row):
    try:
        fields = row.split(",")
        year = get_year(fields[7])
        text = fields[9].lower()
        words = re.findall(r'\b\w{4,}\b', text)
        return ((year, fields[1]), Counter(words))
    except IndexError:
        return None

# Filtra le righe con errore
filtered_data = data.filter(lambda row: row is not None)

# Estrae le parole e le conta
word_counts = filtered_data.map(extract_words).filter(lambda x: x is not None).reduceByKey(lambda a, b: a + b)

# Ordina per anno e conteggio delle recensioni
top_words = word_counts.map(lambda x: (x[0][0], (x[0][1], x[1]))).groupByKey().sortByKey().mapValues(lambda x: sorted(x, key=lambda y: y[1], reverse=True)[:5])

# Stampa i risultati
results = top_words.collect()
for year, word_counts in results:
    print(f"Anno: {year}")
    for word, count in word_counts:
        print(f"Parola: {word}, Occorrenze: {count}")
    print("--------------------")
