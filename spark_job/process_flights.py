# spark_job/process_flights.py

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType

def get_flight_data():
    """Busca dados de voos da API OpenSky Network."""
    url = "https://opensky-network.org/api/states/all"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar dados da API: {e}")
        return None

def main():
    """Função principal do nosso job Spark."""
    spark = SparkSession.builder \
        .appName("FlightDataProcessing") \
        .master("local[*]") \
        .getOrCreate()
    
    print("Sessão Spark criada com sucesso!")

    schema = StructType([
        StructField("icao24", StringType(), True),
        StructField("callsign", StringType(), True),
        StructField("origin_country", StringType(), True),
        StructField("time_position", LongType(), True),
        StructField("last_contact", LongType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("baro_altitude", DoubleType(), True),
        StructField("on_ground", BooleanType(), True),
        StructField("velocity", DoubleType(), True),
        StructField("true_track", DoubleType(), True),
        StructField("vertical_rate", DoubleType(), True),
        StructField("sensors", StringType(), True),
        StructField("geo_altitude", DoubleType(), True),
        StructField("squawk", StringType(), True),
        StructField("spi", BooleanType(), True),
        StructField("position_source", LongType(), True)
    ])
    
    flight_data_json = get_flight_data()

    if flight_data_json and 'states' in flight_data_json and flight_data_json['states']:
        flight_list = flight_data_json['states']
        
        # ------------------ NOVA SEÇÃO: LIMPEZA DE DADOS ------------------
        # Vamos garantir que os tipos de dados estão corretos antes de criar o DataFrame.
        # Índices das colunas que definimos como DoubleType em nosso schema.
        double_indices = {5, 6, 7, 9, 10, 11, 13}
        
        processed_flight_list = []
        for flight_row in flight_list:
            # Convertemos a linha para uma lista para podermos modificar os valores
            processed_row = list(flight_row)
            for index in double_indices:
                if processed_row[index] is not None and isinstance(processed_row[index], int):
                    # Se encontrarmos um inteiro onde deveria ser um double, convertemos para float.
                    processed_row[index] = float(processed_row[index])
            processed_flight_list.append(processed_row)
        # ----------------------------------------------------------------------

        # Usamos a lista processada e limpa para criar o DataFrame
        flights_df = spark.createDataFrame(processed_flight_list, schema=schema)

        print("Schema do DataFrame de Voos (definido por nós):")
        flights_df.printSchema()

        print("Alguns dados de voos recebidos (após limpeza):")
        flights_df.show(5, truncate=False)
        
        df_transformed = flights_df.withColumn(
            "time_position_readable",
            from_unixtime(col("time_position"))
        )

        print("DataFrame com a coluna de tempo transformada:")
        df_transformed.select("callsign", "origin_country", "longitude", "latitude", "time_position_readable").show(5, truncate=False)

    else:
        print("Não foi possível obter dados dos voos ou a resposta veio vazia.")

    spark.stop()

if __name__ == "__main__":
    main()