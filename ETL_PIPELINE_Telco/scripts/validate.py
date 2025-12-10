import pandas as pd

# 1️⃣ Load your transformed CSV
df = pd.read_csv(r"..\data\staged\Telco-Customer_transformed.csv")

# 2️⃣ Normalize column names to lowercase
df.columns = [c.strip().lower() for c in df.columns]

# 3️⃣ Define validation function
def validate_telco_data(df: pd.DataFrame, original_row_count: int):
    print("\n🔍 DATA VALIDATION SUMMARY")
    print("=" * 40)

    # No missing values in key numeric columns
    numeric_cols = ["tenure", "monthlycharges", "totalcharges"]
    missing_numeric = df[numeric_cols].isna().sum()

    print("\n✅ Missing Value Check:")
    for col in numeric_cols:
        if missing_numeric[col] == 0:
            print(f"✔ {col}: No missing values")
        else:
            print(f"❌ {col}: {missing_numeric[col]} missing values")

    # Unique row count vs original
    unique_rows = df.drop_duplicates().shape[0]
    print("\n✅ Uniqueness Check:")
    print(f"✔ Unique rows: {unique_rows}")
    print(f"✔ Original rows: {original_row_count}")
    if unique_rows == original_row_count:
        print("✔ No duplicate rows detected")
    else:
        print("❌ Duplicate rows detected")

    # Row count
    current_rows = df.shape[0]
    print("\n✅ Row Count Check:")
    print(f"✔ Rows in dataset: {current_rows}")
    if current_rows == original_row_count:
        print("✔ Row count matches expected")
    else:
        print("❌ Row count mismatch")

    # Segment checks
    print("\n✅ Segment Validation:")
    print("tenure_group values:")
    print(df["tenure_group"].value_counts())

    print("\nmonthly_charge_segment values:")
    print(df["monthly_charge_segment"].value_counts())

    # Contract codes
    valid_codes = {0, 1, 2}
    found_codes = set(df["contract_type_code"].dropna().unique())
    print("\n✅ Contract Type Code Validation:")
    print(f"✔ Found codes: {found_codes}")
    if found_codes.issubset(valid_codes):
        print("✔ Contract codes are only {0,1,2}")
    else:
        print("❌ Invalid contract codes detected")

    print("\n🎯 VALIDATION COMPLETE")
    print("=" * 40)

# 4️⃣ CALL the function (this is what actually prints)
validate_telco_data(df, original_row_count=7043)
