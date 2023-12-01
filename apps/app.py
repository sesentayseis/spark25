from pyspark.sql import SparkSession

# Configurar la sesión Spark
spark = SparkSession.builder.appName("HolaMundo").getOrCreate()

# Leer el archivo CSV
lines = spark.read.csv("../data/ratings.csv", sep=',', header=True, inferSchema=True)

ratings = lines.rdd.map(lambda x: x[2])

result = ratings.countByValue()

# Mostrar el resultado de la frecuencia de calificaciones
print("Frecuencia de Calificaciones:")
sorted_result = sorted(result.items())

for key, value in sorted_result:
    print(f"Calificación {key}: {value} ocurrencias")

# Mostrar las primeras 5 líneas del DataFrame
for line in lines.take(5):
    print(line)
