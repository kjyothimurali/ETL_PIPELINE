
# ===========================
# transform.py
# ===========================
 
import os
import pandas as pd
import numpy as np

 
# Purpose: Clean and transform Titanic dataset
def transform_data(raw_path):
    # Ensure the path is relative to project root
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # go up one level
    staged_dir = os.path.join(base_dir, "data", "staged")
    os.makedirs(staged_dir, exist_ok=True)
 
    df = pd.read_csv(raw_path)
 
    # --- 1️⃣ Handle missing values ---
    df['TotalCharges']=pd.to_numeric(df['TotalCharges'],errors='coerce')
    df['TotalCharges'].fillna(df['TotalCharges'].median(),inplace=True)
    category_cols=df.select_dtypes(include=['category','object']).columns
    df[category_cols]=df[category_cols].fillna("unknown")
 
    # --- 2️⃣ Feature engineering ---
    df['tenure_group']=np.where(
    df['tenure']<=12,"New",
    np.where(df['tenure']<=36,"Regular",
             np.where(df['tenure']<=60,"Loyal","Champion")
            )
        )
    df['monthly_charge_segment']=np.where(
        df['MonthlyCharges']<30,"Low",
        np.where(df['MonthlyCharges']<=70,"Medium","High"
        )
             
    )
    df["has_internet_service"] = df["InternetService"].isin(
        ["DSL", "Fiber optic"]
    ).astype(int)
    df["is_multi_line_user"] = (df["MultipleLines"] == "Yes").astype(int)
    contract_map = {
        "Month-to-month": 0,
        "One year": 1,
        "Two year": 2
    }

    df["contract_type_code"] = df["Contract"].map(contract_map)
 
    # --- 3️⃣ Drop unnecessary columns ---
    df.drop(columns=["customerID", "gender"], inplace=True)
 
    # --- 4️⃣ Save transformed data ---
    staged_path = os.path.join(staged_dir, "Telco-Customer_transformed.csv")
    df.to_csv(staged_path, index=False)
    print(f"✅ Data transformed and saved at: {staged_path}")
    return staged_path
 
 
if __name__ == "__main__":
    from extract import extract_data
    raw_path = extract_data()
    transform_data(raw_path)
 
 