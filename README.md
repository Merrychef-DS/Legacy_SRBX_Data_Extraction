# ☕ Starbucks Data Pipeline 
******** (Gary's Data extracts → Data Warehouse) ********

This project processes **legacy extracted JSON Starbucks data** and **writes it to a structured PostgreSQL Data Warehouse**.

---

## 📂 **Project Overview**
This pipeline:
1. **Extracts JSON Data** from Gary's Gathers (Raw JSON Files).
2. **Transforms & Cleans Data** (Flattens & Structures it).
3. **Loads Processed Data** into PostgreSQL Data Warehouse.

---

## 🚀 **How to Run the Pipeline**
### **1⃣ Setup Your Environment**
#### **Install Dependencies**
Ensure you have **Python 3+** and install required libraries:
```bash
pip install pandas sqlalchemy psycopg2
```

---

### **2⃣ Extract Data from JSON Files**
1. Place all **JSON files** in:
   ```
   D:\Starbucks_Data\SBUX_portal_data\AllSBSites_AllDates
   ```
2. Specify the **CSV containing serial numbers** (e.g., `sn_48.csv`).
3. Run the extraction script:
   ```bash
   python extract_json.py
   ```
4. **Output:** Cleaned & structured CSVs are saved in:
   ```
   D:\Starbucks_Data\SBUX_portal_data\k_archive_
   ```

---

### **3⃣ Load Data into PostgreSQL Data Warehouse**
1. Ensure PostgreSQL is running and update connection details in `config.py`:
   ```python
   DB_NAME = "starbucks_data"
   DB_USER = "your_username"
   DB_PASSWORD = "your_password"
   DB_HOST = "your_database_host"
   DB_PORT = "5432"
   ```
2. Run the ingestion script to load CSVs into the database:
   ```bash
   python data_extractor.py
   ```
3. **Output:** Data is structured in PostgreSQL under `public` schema.

---

## 📊 **Database Schema**
### **Tables Created**
| Table Name         | Description                           |
|--------------------|---------------------------------------|
| `srbx_errors_data`  | Device error logs                   |
| `srbx_metrics_data` | Performance & product metrics       |
| `srbx_counts_data`  | Device usage stats                  |
| `srbx_state_data`   | Device configurations & state       |
| `srbx_counters_data`| oven counters & operational data |

---

## 🛠 **Troubleshooting**
❌ **Missing JSON Files?**  
Ensure the `start_directory` is correctly set in `extract_json.py`.

❌ **Database Connection Issues?**  
Verify **PostgreSQL credentials** and that the database is running.

❌ **Empty CSVs?**  
Check JSON extraction logs for errors and confirm JSON files have valid data.

---

## 🎯 **Future Improvements**
- Automate **data validation** before insertion.
- Implement **incremental updates** instead of full refresh.
- Optimize **query performance** with indexing.

---

## 🔗 **Author & License**
- **Developed by:** Kirti Gupta
- **License:** MIT License

