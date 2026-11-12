# Distributed Dataset Builder

**A Fault-Tolerant Distributed Pipeline for Continuous Ingestion, Deduplication, and Sharding of Large-Scale Image/Video Datasets for Computer Vision Training**

## 1. Project Overview

This project aims to build a distributed pipeline for continuously ingesting, validating, deduplicating, processing, and storing large-scale image and video datasets for computer vision training.

The system uses Apache Kafka for event-based ingestion and tracks file metadata and processing status using SQLite.

## 2.  Person 1: Anagha C

**Role: Ingestion and Integration**

My responsibilities include:

- Setting up the project repository and Python environment.
- Collecting sample image data for testing.
- Developing a Python producer to detect newly arriving files.
- Integrating Apache Kafka for streaming ingestion.
- Implementing SHA-256-based content deduplication.
- Integrating SQLite for ingestion metadata tracking.
- Coordinating integration with the processing modules.
- Testing whether newly arriving files enter the pipeline successfully.

## 3. Technologies Used

- Python
- Apache Kafka
- Docker
- SQLite
- Ray
- Pillow
- OpenCV
- Streamlit
- Git and GitHub

## 4. Project Structure

```text
distributed-dataset-builder/
├── ingestion/
├── kafka_integration/
├── processing/
├── recovery/
├── storage/
├── validation/
├── tests/
├── data/
├── app.py
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## 5. Setup Instructions

### Step 1: Clone the repository

```bash
git clone https://github.com/anaghac72/distributed-dataset-builder.git
cd distributed-dataset-builder
```

### Step 2: Create a virtual environment

```bash
python -m venv .venv
```

### Step 3: Activate the environment

For Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

### Step 4: Install dependencies

```bash
pip install -r requirements.txt
```

### Step 5: Start the laptop development setup

On a laptop, start Kafka only. SeaweedFS is intentionally kept out of the
default Compose setup so it does not use disk or memory when you are developing
without object storage.

```bash
docker compose config
docker compose up -d
docker compose ps
```

Use a small number of sample images in `data/incoming` for local tests. Kafka
stores its data in the `kafka_data` Docker volume; it does not download the
dataset for you.

### Optional: Start SeaweedFS object storage

Use this only on a server or when you specifically need S3-compatible storage.
Create or update `.env` with unique credentials (do not commit or share it):

```dotenv
SEAWEEDFS_ACCESS_KEY=replace_with_a_unique_access_key
SEAWEEDFS_SECRET_KEY=replace_with_a_long_random_secret
SEAWEEDFS_BUCKET=dataset
```

Generate a secret with `openssl rand -hex 32` and restrict `.env` permissions
with `chmod 600 .env`. Start SeaweedFS alongside Kafka using the storage Compose
file:

```bash
docker compose -f docker-compose.yml -f docker-compose.storage.yml config
docker compose -f docker-compose.yml -f docker-compose.storage.yml up -d
docker compose -f docker-compose.yml -f docker-compose.storage.yml ps
docker compose -f docker-compose.yml -f docker-compose.storage.yml logs --tail=100 seaweedfs
```

SeaweedFS's S3 endpoint is `http://127.0.0.1:8333` on the server, and the
default bucket created at startup is `dataset`. Port 8333 is bound to loopback
so it is not publicly exposed. If your application runs in another container
on this Compose network, use `http://seaweedfs:8333` as its endpoint. For remote
clients, use a secured tunnel or TLS reverse proxy rather than opening the S3
port directly to the internet.

The Python pipeline currently reads and writes local files; a separate S3
integration change is needed before it stores media in SeaweedFS. The previous
MinIO volume is not migrated or deleted by this configuration. Back up or copy
any MinIO data you need before removing the old volume; do not use
`docker compose down -v` until you have confirmed the old data is no longer
needed.

## 6. Ingestion Workflow

1. New files are placed in the incoming directory.
2. The Python producer monitors the directory and validates each new media
   file before ingestion.
3. SHA-256 hashing helps identify duplicate file content.
4. Valid, new ingestion events are published to the Kafka topic.
5. Kafka consumers receive the ingestion events.
6. SQLite stores metadata and processing status.
7. The processing modules handle the received image files.

**Note:** The current tested ingestion workflow focuses on images. Video processing and complete end-to-end video integration require further implementation and testing.

## 7. Testing

The ingestion module was tested with sample image files and newly arriving files.

The tests covered:

- Detection of new files.
- Publishing ingestion events to Kafka.
- Duplicate content identification.
- Metadata tracking in SQLite.
- Integration with the processing workflow.

## 8. Repository

GitHub: https://github.com/anaghac72/distributed-dataset-builder


## 9. Authors

- **Anagha C** — Person 1: Ingestion and Integration
- **Akash S** — Person 2: Pipeline Development and Validation
  
