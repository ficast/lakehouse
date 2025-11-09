FROM apache/spark:3.4.1

# Mude para root temporariamente
USER root

# Crie diretórios necessários e ajuste ownership para o usuário `spark`
RUN mkdir -p /home/spark/.ivy2 /opt/spark/jars \
	&& chown -R spark:spark /home/spark/.ivy2 /opt/spark/jars

# Instale as libs Python (sem cache para manter imagem menor)
RUN pip install --no-cache-dir pyspark==3.4.1 delta-spark==3.2.0 kafka-python==2.0.2 pandas

# Retorne ao usuário padrão
USER spark

WORKDIR /opt/spark-apps

# Baixar jars que o cluster precisa em tempo de execução para evitar dependências via Ivy
# (kafka connector + delta). Colocamos os jars em /opt/spark/jars para serem carregados pelo Spark.
USER root
RUN set -eux; \
		cd /opt/spark/jars; \
		# remove any jars built for Scala 2.13 to avoid runtime mismatch with Spark's Scala version
		rm -f *\_2.13-* || true; \
		# download spark-sql-kafka and token-provider built for Scala 2.12 (match Spark default)
		curl -fL -o spark-sql-kafka-0-10_2.12-3.4.1.jar \
			https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.1/spark-sql-kafka-0-10_2.12-3.4.1.jar; \
		curl -fL -o spark-token-provider-kafka-0-10_2.12-3.4.1.jar \
			https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.1/spark-token-provider-kafka-0-10_2.12-3.4.1.jar; \
		# kafka clients (recent) and delta core/storage for Scala 2.12
		curl -fL -o kafka-clients-3.3.2.jar \
			https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.2/kafka-clients-3.3.2.jar; \
			# commons-pool2 is required by some Kafka utilities
			curl -fL -o commons-pool2-2.11.1.jar \
				https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar; \
		curl -fL -o delta-core_2.12-2.4.0.jar \
			https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar; \
		curl -fL -o delta-storage-2.3.0.jar \
			https://repo1.maven.org/maven2/io/delta/delta-storage/2.3.0/delta-storage-2.3.0.jar; \
		chown -R spark:spark /opt/spark/jars
USER spark
