
# Bacth Processing using PySpark and Airflow
This repository contains examples of batch processing pipeline implementations using PySpark and Apache Airflow

![Deskripsi Gambar](https://file.notion.so/f/f/01b24fa3-f906-4bbc-ae9a-eae8c32be7d8/32c7269a-4849-4425-bf0d-a7c1fb667885/image.png?table=block&id=0fc17910-9ab2-4e12-9d21-b0bd32002c60&spaceId=01b24fa3-f906-4bbc-ae9a-eae8c32be7d8&expirationTimestamp=1734746400000&signature=blhzvGDfjtVAWHw5qP0XH35BMmaLqkKNeEB3p2D2O4g&downloadName=image.png)

## Requirements
Before you begin, make sure you have installed:
- [Docker](https://www.docker.com/get-started)
- [Docker Compose](https://docs.docker.com/compose/install/)

## 🚀 Steps

1. Clone this repository:
```bash
   git clone https://github.com/ahfay/Batch-Processing-using-PySpark-and-Airflow.git
   cd Batch-Processing-using-PySpark-and-Airflow
```
2. Change the **example.env** file to **.env**
```bash
   mv example.env .env
```
3. Build images
```bash
   docker build . -t my-airflow
```
4. Run the service using Docker Compose
```bash
   docker compose up -d
```
5. Rename the file **~/dags/modules/example.credential.json** to **~/dags/modules/credential.json**, and then change the configuration to match the one you are using

6. Open a browser and navigate to **http://localhost:8080** to access Airflow's web interface.

7. In the Airflow web interface, enable and run the desired DAG to start the batch processing pipeline.
