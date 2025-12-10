import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

# Load env
load_dotenv()
supabase = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY")
)

# Read table from Supabase
data = supabase.table("telco_customer_churn_features").select("*").execute().data
df = pd.DataFrame(data)

# Normalize column names
df.columns = [c.lower() for c in df.columns]

# =====================
# METRICS
# =====================

summary = []

# 1. Churn percentage
churn_pct = (df["churn"].str.lower() == "yes").mean() * 100
summary.append(["churn_percentage", churn_pct])

# 2. Average monthly charges per contract
avg_monthly = df.groupby("contract")["monthlycharges"].mean()
for k, v in avg_monthly.items():
    summary.append([f"avg_monthlycharges_{k}", v])

# 3. Tenure group counts
tenure_counts = df["tenure_group"].value_counts()
for k, v in tenure_counts.items():
    summary.append([f"tenure_group_{k}_count", v])

# 4. Internet service distribution
internet_counts = df["internetservice"].value_counts()
for k, v in internet_counts.items():
    summary.append([f"internet_{k}_count", v])

# 5. Pivot: Churn vs Tenure Group
pivot = pd.crosstab(df["tenure_group"], df["churn"])
pivot.to_csv("../data/processed/churn_vs_tenure_group.csv")

# =====================
# SAVE SUMMARY
# =====================

summary_df = pd.DataFrame(summary, columns=["metric", "value"])
os.makedirs("../data/processed", exist_ok=True)
summary_df.to_csv("../data/processed/analysis_summary.csv", index=False)

print("✅ Analysis completed and saved")
