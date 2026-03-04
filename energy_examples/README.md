## Assessment Solution Overview

I have completed the assessment. Please find below the details of my solution and the attached code.

### Architecture & Approach

I opted to use **Databricks SDP** for this solution, which consists of:
- **2 pipelines**
- **1 job**
- Follows a typical **medallion architecture**

I chose SDP for its simplicity (despite frequent name changes) and the ease of applying data expectations to monitor data quality. The solution is wrapped in a simple Databricks Asset Bundle.

### Data Assumptions (from `data exploration.py`)

- **Turbine ID**: Always present
- **Wind Speed**: Always present and ≥ 0
- **Wind Direction**: Always present and ≥ 0
- **Power Output**: Always present and ≥ 0
- **New files**: Arrive daily for each data group, covering the last 24 hours

---

### Solution Components

#### Pipeline 1: `historical_turbine_data_sdp.py`
- Maintains lower and upper bounds of power output per turbine (±2 standard deviations from the mean)
- Provides an overall view of bounds by turbine for all available data
- Updated each time the silver table is updated
- Used to determine if new data falls within these bounds

#### Pipeline 2: `turbine_data_sdp.py`
- Ingests data from raw using structured streaming, creating a bronze table with the latest data
- Applies expectations based on the assumptions above; flags records for quarantine
- Stores all validated records in the silver table, including a quarantine flag for easy identification of bad data (can be stored elsewhere or dropped as needed)
- Valid records from the latest batch are used to create the gold statistics table (min, max, average power output by turbine)
- Gold table is aggregated by turbine and day, providing historical performance

#### Job: `orchestrate_turbine_data.yaml`
- Runs Pipeline 1, then Pipeline 2