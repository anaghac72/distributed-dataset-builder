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

### Step 5: Start Kafka using Docker Compose

```bash
docker compose up -d
```

## 6. Ingestion Workflow

1. New files are placed in the incoming directory.
2. The Python producer monitors the directory for new files.
3. SHA-256 hashing helps identify duplicate file content.
4. New ingestion events are published to the Kafka topic.
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

## 9. Author

**Anagha C**  
Person 1 – Ingestion and Integration
