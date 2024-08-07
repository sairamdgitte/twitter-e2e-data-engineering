# Realtime Data Streaming | End-to-End Data Engineering Project

## Table of Contents
- [Introduction](#introduction)
- [System Architecture](#system-architecture)
- [High-level workflow](#what-youll-learn)
- [Technologies](#technologies)
- [Getting Started](#getting-started)
- [Watch the Video Tutorial](#watch-the-video-tutorial)

## Introduction

This project serves as a comprehensive guide to building an end-to-end data engineering pipeline. It covers each stage from data ingestion to processing and finally to storage, utilizing a robust tech stack that includes Apache Airflow, Python, Apache Kafka, Apache Zookeeper, Apache Spark, and Cassandra. Everything is containerized using Docker for ease of deployment and scalability.

## System Architecture

![System Architecture](media/System-architecture.png)

The project is designed with the following components:

- **Data Source**: I have used a health-related tweets' database which I scraped in 2022 - with 109k tweets.  
- **Apache Airflow**: Responsible for orchestrating the pipeline and storing fetched data in a PostgreSQL database.
- **Apache Kafka and Zookeeper**: Used for streaming data from PostgreSQL to the processing engine.
- **Control Center and Schema Registry**: Helps in monitoring and schema management of our Kafka streams.
- **Apache Spark**: For data processing with its master and worker nodes.
- **Cassandra**: Where the processed data will be stored.

## High-level workflow

- Setting up a data pipeline with Apache Airflow
- Real-time data streaming with Apache Kafka
- Distributed synchronization with Apache Zookeeper
- Data processing techniques with Apache Spark
- Data storage solutions with Cassandra and PostgreSQL
- Containerizing your entire data engineering setup with Docker

## Technologies

- Apache Airflow
- Python
- Apache Kafka
- Apache Zookeeper
- Apache Spark
- Cassandra
- PostgreSQL
- Docker

## Getting Started

1. Clone the repository:
    ```bash
    git clone https://github.com/sairamdgitte/twitter-e2e-data-engineering.git
    ```

2. Navigate to the project directory:
    ```bash
    cd twitter-e2e-data-engineering
    ```

3. Run Docker Compose to spin up the services:
    ```bash
    docker-compose up
    ```


<!-- ![Twitter Dashboard](media/twitter-dashboard.mp4) -->

<iframe width="560" height="315" src="https://youtu.be/Xc2-4A1BNjs" frameborder="0" allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>