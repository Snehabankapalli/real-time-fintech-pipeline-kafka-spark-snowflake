# Real-Time Fintech Pipeline - Architecture Diagrams

## System Flow

```mermaid
graph LR
    A["💳 Card Events<br/>- Authorizations<br/>- Settlements<br/>- Refunds<br/>- Chargebacks"]
    B["🚀 Apache Kafka<br/>- 6 partitions<br/>- Schema Registry<br/>- DLQ support"]
    C["⚡ PySpark EMR<br/>Serverless<br/>- Parse<br/>- Validate<br/>- Tokenize PII<br/>- Enrich<br/>- Deduplicate"]
    D["❄️ Snowflake<br/>- RAW<br/>- STAGING<br/>- INTERMEDIATE<br/>- MARTS"]
    E["📊 Dashboards<br/>- BI Tools<br/>- CFPB Reports<br/>- Fraud Detection"]

    A -->|1M+ events/day| B
    B -->|Schema validated| C
    C -->|10s micro-batch| D
    D -->|Processed data| E

    style A fill:#e1f5ff
    style B fill:#fff3e0
    style C fill:#f3e5f5
    style D fill:#e8f5e9
    style E fill:#fce4ec
```

## Data Flow Layers

```mermaid
graph TB
    subgraph "Ingestion Layer"
        A["Kafka MSK<br/>- Transactions Topic<br/>- DLQ Topic"]
    end

    subgraph "Processing Layer"
        B["PySpark Streaming<br/>- Micro-batches: 10s<br/>- Exactly-once<br/>- PII Tokenization"]
    end

    subgraph "Storage Layer"
        C["Snowflake<br/>- Raw Tables<br/>- Staging Views<br/>- Intermediate MVs"]
    end

    subgraph "Transformation Layer"
        D["dbt Models<br/>- Staging (views)<br/>- Intermediate (mvs)<br/>- Marts (tables)"]
    end

    subgraph "Analytics Layer"
        E["Executive Dashboards<br/>CFPB Reports<br/>Fraud Signals"]
    end

    A -->|Streaming| B
    B -->|Write| C
    C -->|Transform| D
    D -->|Serve| E

    style A fill:#ffecb3
    style B fill:#e1bee7
    style C fill:#c8e6c9
    style D fill:#b3e5fc
    style E fill:#ffccbc
```

## Technology Stack

```mermaid
graph LR
    subgraph "Cloud"
        AWS["AWS EMR Serverless<br/>Lambda, S3, Secrets Manager"]
    end

    subgraph "Streaming"
        KAFKA["Apache Kafka<br/>Schema Registry"]
    end

    subgraph "Processing"
        SPARK["PySpark<br/>Structured Streaming"]
    end

    subgraph "Warehouse"
        SF["Snowflake<br/>Zero-copy Clones<br/>Result Caching"]
    end

    subgraph "Transformation"
        DBT["dbt<br/>Version Control<br/>Testing"]
    end

    subgraph "Orchestration"
        AIRFLOW["Apache Airflow<br/>SLA Callbacks<br/>Retry Logic"]
    end

    subgraph "Monitoring"
        DATADOG["Datadog<br/>CloudWatch<br/>Prometheus"]
    end

    KAFKA --> SPARK
    SPARK --> SF
    SF --> DBT
    DBT --> AIRFLOW
    SPARK -.-> DATADOG
    SF -.-> DATADOG
    AIRFLOW -.-> DATADOG
```

## Kafka Partitioning Strategy

```mermaid
graph TB
    A["Transaction Events"] -->|Hash by account_id| B["Partition 0<br/>Account A"]
    A -->|Hash by account_id| C["Partition 1<br/>Account B"]
    A -->|Hash by account_id| D["Partition 2<br/>Account C"]
    A -->|Hash by account_id| E["Partition 3<br/>DLQ"]

    B --> F["Guaranteed Order<br/>Per Account"]
    C --> F
    D --> F
    E --> G["Error Isolation<br/>Replay Support"]
```

## dbt Transformation Layers

```mermaid
graph TB
    subgraph "RAW"
        A["raw_transactions<br/>raw_payments"]
    end

    subgraph "STAGING (Views)"
        B["stg_transactions<br/>stg_payments<br/>- JSON parsing<br/>- Deduplication"]
    end

    subgraph "INTERMEDIATE (MVs)"
        C["int_transactions_enriched<br/>- Joins<br/>- Derived fields<br/>- Incremental merge"]
    end

    subgraph "MARTS (Tables)"
        D["fct_transactions<br/>fct_daily_metrics<br/>rpt_cfpb_monthly<br/>rpt_fraud_signals<br/>- Clustered<br/>- Cached"]
    end

    A --> B
    B --> C
    C --> D

    style A fill:#ffecb3
    style B fill:#fff9c4
    style C fill:#f0f4c3
    style D fill:#dcedc8
```

## Cost Optimization Flow

```mermaid
graph LR
    A["Raw Data<br/>100GB/day"]
    B["Partitioned S3<br/>90-day retention<br/>Lower tier"]
    C["Snowflake Clustering<br/>By event_date"]
    D["Result Caching<br/>$0 repeated queries"]
    E["Zero-copy Clones<br/>Dev/staging free"]

    A -->|83% faster| B
    B -->|60% query speedup| C
    C -->|Repeated queries| D
    D -->|Cloning| E

    style A fill:#ffcdd2
    style B fill:#f8bbd0
    style C fill:#f48fb1
    style D fill:#ec407a
    style E fill:#c2185b
```

## SLA and Monitoring

```mermaid
graph LR
    A["Kafka<br/>Monitoring"]
    B["Spark<br/>Monitoring"]
    C["Snowflake<br/>Monitoring"]
    D["dbt<br/>Monitoring"]

    A -->|Lag, Throughput| E["Datadog<br/>Dashboards"]
    B -->|Batch Duration| E
    C -->|Query Time| E
    D -->|Test Results| E

    E -->|Alerts| F["Slack<br/>Notifications"]
    E -->|Metrics| G["Executive<br/>Reports"]

    style E fill:#e3f2fd
    style F fill:#ffebee
    style G fill:#f3e5f5
```
