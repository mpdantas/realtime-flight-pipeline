# spark_job/process_flights.py

# Importações necessárias - GARANTA QUE TODAS ESTEJAM AQUI
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
    
    # 1. Criando a SparkSession (sem a linha .config)
    # O Spark encontrará o driver JDBC automaticamente na pasta /opt/bitnami/spark/jars
    spark = SparkSession.builder \
        .appName("FlightDataProcessing") \
        .master("local[*]") \
        .getOrCreate()
    
    print("Sessão Spark criada com sucesso!")

    # 2. Definindo o Schema Explícito
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
        
        # 3. Limpeza de Dados (Pré-processamento)
        double_indices = {5, 6, 7, 9, 10, 11, 13}
        processed_flight_list = []
        for flight_row in flight_list:
            processed_row = list(flight_row)
            for index in double_indices:
                if processed_row[index] is not None and isinstance(processed_row[index], int):
                    processed_row[index] = float(processed_row[index])
            processed_flight_list.append(processed_row)

        flights_df = spark.createDataFrame(processed_flight_list, schema=schema)
        
        df_transformed = flights_df.withColumn(
            "time_position_readable",
            from_unixtime(col("time_position"))
        ).withColumn(
            "last_contact_readable",
            from_unixtime(col("last_contact"))
        )
        
        # 4. Escrevendo no Banco de Dados PostgreSQL
        print("Iniciando escrita dos dados no PostgreSQL...")
        
        df_to_write = df_transformed.select(
            "icao24", "callsign", "origin_country", "longitude", "latitude", 
            "baro_altitude", "on_ground", "velocity", "true_track", 
            "vertical_rate", "geo_altitude", "squawk", "time_position_readable", "last_contact_readable"
        ).filter(col("callsign").isNotNull())

        db_properties = {
            "user": "datateam",
            "password": "datateam",
            "driver": "org.postgresql.Driver"
        }
        
        # Use a URL com o IP que encontrámos anteriormente
        db_url = "jdbc:postgresql://172.18.0.3:5432/flights" # MANTENHA O IP AQUI

        df_to_write.write.jdbc(
            url=db_url,
            table="flight_data_live",
            mode="overwrite",
            properties=db_properties
        )

        print(f"{df_to_write.count()} registros de voos escritos com sucesso na tabela 'flight_data_live'!")

    else:
        print("Não foi possível obter dados dos voos ou a resposta da API veio vazia.")

    spark.stop()

if __name__ == "__main__":
    main()
