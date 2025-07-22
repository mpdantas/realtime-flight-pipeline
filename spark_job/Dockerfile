# 1. Imagem Base
# Usamos uma imagem pronta da Bitnami que já vem com Spark e Python.
# Isso economiza muito tempo de configuração. A versão do Spark aqui (3.5.1)
# é a mesma que especificamos no requirements.txt para consistência.
FROM bitnami/spark:3.5.1

# 2. Definir o Diretório de Trabalho
# Define o diretório padrão dentro do contêiner. Todos os comandos seguintes
# serão executados a partir daqui.
WORKDIR /app

# 3. Copiar e Instalar as Dependências
# Copiamos o requirements.txt primeiro e instalamos. O Docker constrói em camadas,
# e fazer isso agora permite que o Docker reutilize a camada de dependências instaladas
# caso a gente mude apenas nosso script, tornando futuros builds mais rápidos.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copiar o Código da Aplicação
# Agora copiamos o nosso script para dentro do contêiner.
COPY process_flights.py .

# 5. Comando de Execução
# Define o comando que será executado quando o contêiner iniciar.
# 'spark-submit' é a forma padrão de executar uma aplicação Spark.
CMD ["spark-submit", "process_flights.py"]