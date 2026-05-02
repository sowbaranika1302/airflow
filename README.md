# 🚀 NASA Asteroid ETL — Apache Airflow Pipeline

An Apache Airflow DAG that extracts NASA's **Astronomy Picture of the Day (APOD)** data from the NASA API, transforms it, and loads it into a PostgreSQL database on a daily schedule.

---

## 📋 Overview

This pipeline automates the daily ingestion of NASA APOD data using an ETL (Extract, Transform, Load) workflow. Despite the DAG ID referencing "asteroids," this pipeline specifically targets the APOD endpoint, storing each day's featured astronomy image metadata into a PostgreSQL table.

**Pipeline Steps:**
1. Create the target PostgreSQL table (if it doesn't already exist)
2. Extract data from the NASA APOD API
3. Transform the raw API response into a structured format
4. Load the transformed data into PostgreSQL

---

## 🗂️ Project Structure

```
.
├── dags/
│   └── nasa_asteroid_etl.py   # Main Airflow DAG
└── README.md
```

---

## ⚙️ Prerequisites

- Python 3.8+
- Apache Airflow 2.x
- PostgreSQL database
- The following Airflow providers:
  - `apache-airflow-providers-http`
  - `apache-airflow-providers-postgres`

Install the providers with:
```bash
pip install apache-airflow-providers-http apache-airflow-providers-postgres
```

---

## 🔌 Airflow Connections

Configure the following connections in the Airflow UI (**Admin → Connections**):

### 1. NASA API Connection
| Field        | Value                              |
|--------------|------------------------------------|
| Connection ID | `nasa_api`                        |
| Connection Type | `HTTP`                          |
| Host         | `https://api.nasa.gov`             |
| Extras (JSON) | `{"api_key": "YOUR_NASA_API_KEY"}` |

> Get your free API key at [https://api.nasa.gov](https://api.nasa.gov)

### 2. PostgreSQL Connection
| Field         | Value                    |
|---------------|--------------------------|
| Connection ID | `postgres_connection`    |
| Connection Type | `Postgres`             |
| Host          | `your_postgres_host`     |
| Schema        | `your_database_name`     |
| Login         | `your_username`          |
| Password      | `your_password`          |
| Port          | `5432`                   |

---

## 🗄️ Database Schema

The DAG automatically creates the following table on first run:

```sql
CREATE TABLE IF NOT EXISTS asteroids (
    id         SERIAL PRIMARY KEY,
    name       VARCHAR(255),
    explanation TEXT,
    url        TEXT,
    date       DATE,
    media_type VARCHAR(50)
);
```

---

## 🔄 DAG Configuration

| Property       | Value              |
|----------------|--------------------|
| DAG ID         | `Nasa_Asteroid_ETL` |
| Schedule       | `@daily`           |
| Start Date     | `2026-05-01`       |
| Catchup        | `False`            |

---

## 🧩 Task Breakdown

| Task                  | Type              | Description                                        |
|-----------------------|-------------------|----------------------------------------------------|
| `create_asteroid_table` | `@task` (Python) | Creates the `asteroids` table if it doesn't exist  |
| `extract_apod`        | `HttpOperator`    | Calls the NASA APOD API and returns JSON response  |
| `transform_apod_data` | `@task` (Python)  | Maps API fields to the database schema             |
| `load_apod_data`      | `@task` (Python)  | Inserts the transformed record into PostgreSQL     |

### Task Dependency Graph

```
create_asteroid_table >> extract_apod >> transform_apod_data >> load_apod_data
```

---

## 🚀 Getting Started

1. **Clone the repository** and place the DAG file in your Airflow `dags/` folder.

2. **Configure connections** in the Airflow UI as described above.

3. **Enable the DAG** in the Airflow UI and trigger a manual run to verify the setup:
   ```bash
   airflow dags trigger Nasa_Asteroid_ETL
   ```

4. **Verify the data** by querying your PostgreSQL database:
   ```sql
   SELECT * FROM asteroids ORDER BY date DESC LIMIT 10;
   ```

---

## 🛠️ Troubleshooting

- **API key errors:** Confirm the `nasa_api` connection Extras JSON is valid and the key is active.
- **PostgreSQL connection failures:** Check that `postgres_connection` credentials and host are correct, and that the Airflow worker can reach the database.
- **Empty `url` fields:** APOD occasionally returns video content. The `url` will still be populated, but `media_type` will be `video` instead of `image`.

---

## 📄 License

This project is open-source and available under the [MIT License](LICENSE).
