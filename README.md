# 🏗️ Projeto Lakehouse – IACD  
### Mestrado em Inteligência Artificial e Ciência de Dados  
**Universidade de Coimbra**

---

## 🚀 Sobre o Projeto

Este projeto demonstra a implementação de um **Data Lakehouse** moderno, utilizando:

- **Kafka** → ingestão de eventos em streaming  
- **Spark + Delta Lake** → processamento, persistência e qualidade de dados  
- **Camadas Bronze / Silver / Gold** → arquitetura em níveis para confiabilidade e governança  
- **Grafana** → visualização quase em tempo real  

O objetivo é construir uma pipeline de dados **fim a fim**, do streaming à análise.

---

## 🏭 Case: *Smart Factory*

Uma fábrica com **200 máquinas industriais** envia, em tempo real, medições de:
- Temperatura  
- Vibração  
- Consumo energético  
- Contagem de produção  

Esses dados são ingeridos, processados e disponibilizados em camadas para análise de desempenho e manutenção preditiva.

---

## ⚙️ Stack Tecnológica

| Componente | Função |
|-------------|---------|
| **Kafka** | Ingestão de eventos em tempo real |
| **Spark (com Delta Lake)** | Processamento e persistência confiável |
| **Grafana** | Visualização dos dados das camadas superiores |
| **Python + Faker** | Simulação dos eventos de fábrica |

---

## 🧩 Arquitetura Medallion

| Camada | Tipo | Descrição |
|---------|------|------------|
| **Bronze** | Streaming | Dados crus, ingeridos do Kafka |
| **Silver** | Batch (1 min)** | Dados limpos e enriquecidos |
| **Gold** | Batch (5 min)** | Agregações e KPIs por máquina/hora |

> ⚙️ Frequências de atualização configuráveis — ideais para demonstração.

---

## 🧰 Como Iniciar

### 1️⃣ Configuração inicial

Crie o arquivo de ambiente:

```bash
cp .env.example .env
````

### 2️⃣ Suba a stack

```bash
docker compose up -d
```

Isso inicia **Kafka**, **Spark** e **Grafana**.

---
## Geração das máquinas

Gerador do banco de dados de máquinas disponível (arquivo estático). 
Só precisa ser gerado uma única vez.

```bash
uv run src/generators/generate_machines.py
```


## 🔄 Simulação de Eventos

Gerador de dados das máquinas (executa localmente):

```bash
uv run src/generators/generate_machine_events.py
```

Esse script:
- Cria (se necessário) o tópico `machine_events`
- Envia eventos contínuos de cada máquina para o Kafka

---

## Consumer (Para debug)

```bash
docker exec -it kafka kafka-console-consumer \
  --topic machine_events \
  --bootstrap-server localhost:9092 \
  --from-beginning
```


## 🪣 Ingestão – Camada Bronze

O job **Bronze** lê os eventos do Kafka e grava no Delta Lake, em `/data/bronze/machine_events`.

Execute dentro do container Spark:

```bash
docker exec -it spark bash -lc "spark-submit --conf 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension' --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog' /opt/spark-apps/jobs/bronze/ingest_machine_events.py"
```

---

## 🧮 Processamento – Camada Silver

Job batch executado a cada minuto (pode ser agendado via cron):

```bash
docker exec spark spark-submit /opt/spark-apps/jobs/silver/transform_machine_events.py
```

Responsável por:
- Limpeza de duplicados/nulos  
- Tipagem de colunas  
- Enriquecimento com metadados das máquinas  

---

## 📊 Agregação – Camada Gold

Job executado a cada 5 minutos:

```bash
docker exec spark spark-submit /opt/spark-apps/jobs/gold/aggregate_machine_kpis.py
```

Calcula KPIs:
- Temperatura média por máquina/hora  
- Total de falhas  
- Taxa de produção  

---

## 📈 Visualização no Grafana

O Grafana se conecta à camada **Gold** (ou Silver) para exibir dashboards em tempo quase real.

Acesse:
```text
http://localhost:3000
```

> Login padrão: **admin / admin**

---

## 🧠 Conceitos Demonstrados

✅ Ingestão streaming com Kafka  
✅ Processamento com Spark Structured Streaming  
✅ Arquitetura Medallion (Bronze/Silver/Gold)  
✅ Persistência transacional com Delta Lake  
✅ Job orchestration (cron/compose)  
✅ Visualização de métricas industriais  

---

## 🧑‍💻 Autores

**Filipe de Castro Oliveira**  
**Emanuel Dias Pacheco**  
*Mestrado em Inteligência Artificial e Ciência de Dados*  
*Universidade de Coimbra*
