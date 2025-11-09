FROM confluentinc/cp-kafka:8.1.0

# Alterna pra root pra conseguir alterar permissões
USER root

# Copia e garante permissão de execução
COPY create-topics.sh /usr/local/bin/create-topics.sh
RUN chmod +x /usr/local/bin/create-topics.sh

# Volta para o usuário padrão usado pela imagem (appuser)
USER appuser

# Sobrescreve o entrypoint padrão: inicia o Kafka e depois cria o tópico
CMD ["/bin/bash", "-c", "/etc/confluent/docker/run & sleep 15 && /usr/local/bin/create-topics.sh && wait"]
