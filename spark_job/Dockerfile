# 1. Imagem Base
FROM bitnami/spark:3.5.1

# --- NOVA ETAPA: Mudar para o utilizador root e instalar o curl ---
# Precisamos de ser root para instalar pacotes.
USER root
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# 2. Descarregar e Instalar o Driver JDBC do PostgreSQL
# Agora que o curl está instalado, este comando vai funcionar.
RUN curl -L -o /opt/bitnami/spark/jars/postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

# 3. Definir o Diretório de Trabalho
WORKDIR /app

# 4. Copiar e Instalar as Dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copiar o Código da Aplicação
COPY process_flights.py .

# 6. Comando de Execução
CMD ["spark-submit", "process_flights.py"]
