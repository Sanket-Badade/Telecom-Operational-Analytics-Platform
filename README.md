# Telecom-Operational-Analytics-Platform

##(currently working on this project)

## RAW ARCHITECTURE

                            ┌────────────────────────────────────────────────────────┐
                            │                   1. DATA SOURCES                      │        ------------>> completed
                            │  • Streaming JSON  • Nested API  • SQL DB  • 2x CSVs   │
                            └───────────────────────────┬────────────────────────────┘
                                                        │
                                                [ADF / Event Hub]
                                                        │
                                                        ▼
                            ┌────────────────────────────────────────────────────────┐
                            │                   2. LANDING ZONE                      │      -------------->> completed
                            │             (Immutable Raw Blob Storage)               │
                            └───────────────────────────┬────────────────────────────┘
                                                        │
                                             [Spark Read Mode Rules]
                                             ├── Stream  --> Append
                                             └── Batch   --> Overwrite
                                                        │
                                                        ▼
                            ┌────────────────────────────────────────────────────────┐
                            │                   3. BRONZE LAYER                      │    --------------->> completed
                            │               (Raw Delta Lake Tables)                  │
                            └───────────────────────────┬────────────────────────────┘
                                                        │
                                             [Data Quality & Joins]
                                             ├── Quarantining (Bad Data)
                                             └── SCD Type 2 (Assets)
                                                        │
                                                        ▼
                            ┌────────────────────────────────────────────────────────┐
                            │                   4. SILVER LAYER                      │
                            │         (Cleaned, Enriched, Validated Tables)          │
                            └───────────────────────────┬────────────────────────────┘
                                                        │
                                              [Memory-Optimized Joins]
                                             └── Broadcast Dim Tables
                                                        │
                                                        ▼
                            ┌────────────────────────────────────────────────────────┐
                            │                    5. GOLD LAYER                       │
                            │         (Production Operational Analytics Mart)        │
                            └────────────────────────────────────────────────────────┘
