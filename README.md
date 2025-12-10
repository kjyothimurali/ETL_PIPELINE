# ETL_PIPELINE

# ETL Pipeline Project  
**Titanic Dataset & Telco Customer Churn Dataset**

## 📌 Project Overview
This project demonstrates a complete **ETL (Extract, Transform, Load) pipeline** built using **Python, Pandas, and Supabase (PostgreSQL)**.  
The pipeline processes two real-world datasets:

- 🚢 **Titanic Dataset** – Passenger survival data
- 📡 **WA_Fn-UseC_-Telco-Customer-Churn Dataset** – Customer churn analytics

The goal is to perform data extraction, cleaning, feature engineering, validation, loading into a cloud database, and basic analysis.

---

## 🧩 Datasets Used

### 1️⃣ Titanic Dataset
- Passenger demographics
- Ticket, fare, cabin details
- Survival label

### 2️⃣ Telco Customer Churn Dataset
- Customer demographics
- Service subscriptions
- Contract and payment details
- Churn status

---

## 🏗️ ETL Pipeline Architecture

Raw Data
↓
Extract (CSV)
↓
Transform (Cleaning + Feature Engineering)
↓
Validate (Data Quality Checks)
↓
Load (Supabase PostgreSQL)
↓
Analyze (Metrics & Summary Reports)

## ⚙️ Technologies Used
- **Python 3**
- **Pandas**
- **Supabase (PostgreSQL)**
- **dotenv**
- **Git & GitHub**

  ## 📂 Project Structure

  ETL_PIPELINE/
│
├── data/
│ ├── raw/ # Original datasets
│ ├── staged/ # Transformed datasets
│ └── processed/ # Analysis outputs
│
├── scripts/
│ ├── extract.py # Data extraction
│ ├── transform.py # Cleaning & feature engineering
│ ├── load.py # Load data into Supabase
│ ├── etl_analysis.py # Analysis & metrics
│ └── etl_validation.py # Data validation checks
│
├── .env # Supabase credentials
├── requirements.txt
└── README.md


---

## 🔄 ETL Steps Explained

### ✅ Extract
- Load raw CSV files using Pandas.

### ✅ Transform
- Handle missing values.
- Convert data types.
- Feature engineering:
  - `tenure_group`
  - `monthly_charge_segment`
  - `has_internet_service`
  - `contract_type_code`
- Encode categorical variables.

### ✅ Validate
- No missing values in critical numeric fields.
- Row count consistency.
- Valid category segments.
- Contract codes limited to `{0,1,2}`.

### ✅ Load
- Load transformed data into **Supabase PostgreSQL**.
- Batch inserts with retry logic.
- NaN → NULL handling.

### ✅ Analyze
- Churn percentage
- Average monthly charges per contract
- Tenure group distribution
- Internet service distribution
- Churn vs Tenure Group pivot
- Output saved as CSV

---

## 📊 Sample Analysis Metrics
- Overall churn rate
- Churn by tenure group
- Contract-wise monthly charges
- Customer segmentation insights

---

## 🗄️ Database
- **Supabase PostgreSQL**
- Cloud-hosted
- Table: `telco_customer_churn_features`

---

## 🚀 How to Run

1. Clone the repository
   ```bash
   git clone <repo-url>

2. Install dependencies

pip install -r requirements.txt


3. Configure .env

SUPABASE_URL=your_url
SUPABASE_KEY=your_key


4. Run ETL

python scripts/extract.py
python scripts/transform.py
python scripts/load.py
python scripts/etl_analysis.py
