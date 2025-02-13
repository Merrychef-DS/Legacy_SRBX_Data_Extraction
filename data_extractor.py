import json
import os
import pandas as pd
import re
import psycopg2
#from psycopg2 import sql
from datetime import datetime
from sqlalchemy import create_engine, Table, Column, MetaData, String, Date, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError, NoSuchTableError


# Global variable to store last known address content
# address_content = ""

# List to store JSON decoding errors
json_error_logs = []
file_dates = []
twin_data_list = []
        
# PostgreSQL connection details
DB_NAME = "starbucks_data"
DB_USER = "gk105768"
DB_PASSWORD = "Welbilt2024!"
DB_HOST = "muk-npi-db001.postgres.database.azure.com"
DB_PORT = "5432"
SCHEMA_NAME = 'public'

# # Define your PostgreSQL connection parameters
# DB_HOST = 'muk-az-db001.postgres.database.azure.com'
# DB_PORT = 5432
# DB_NAME = "merrychef-dw"
# DB_USER = 'service_insights_rw'
# DB_PASSWORD = "xDQ?YLZ]9u7nCqmwW~;A'^"
# schema = 'usr_service_insights'


# Function to log JSON decoding errors
def log_json_error(file_path, error_message):
    try:
        with open("json_decoding_errors.txt", 'a') as log_file:
            log_file.write(f"File: {file_path}\n")
            log_file.write(f"Error: {error_message}\n\n")
    except Exception as e:
        print(f"Error logging JSON error: {e}")
        # raise


def get_create_table_sql(table_name, schema):
    table_schemas = {
        "srbx_errors_data": f"""
            CREATE TABLE IF NOT EXISTS {schema}.srbx_errors_data (
                address TEXT,
                date DATE,
                device_type TEXT,
                serial_number TEXT,
                error_code TEXT,
                error_time TEXT,
                error_event TEXT,
                error_description TEXT,
                error_status TEXT,
                error_details TEXT
            );
        """,
        "srbx_products_data": f"""
            CREATE TABLE IF NOT EXISTS {schema}.srbx_products_data (
                device_type TEXT,
                serial_number TEXT,
                product_type TEXT,
                results_time TEXT,
                results_recipe_name TEXT,
                results_status TEXT,
                address TEXT,
                date DATE
            );
        """,
        "srbx_metrics_data": f"""
            CREATE TABLE IF NOT EXISTS {schema}.srbx_metrics_data (
                device_type TEXT,
                serial_number TEXT,
                date DATE,
                total_products TEXT,
                total_errors TEXT,
                address TEXT
            );
        """,
        "srbx_counts_data": f"""
            CREATE TABLE IF NOT EXISTS {schema}.srbx_counts_data (
                device_type TEXT,
                serial_number TEXT,
                date DATE,
                device_heartbeats TEXT,
                gm_heartbeats TEXT,
                products TEXT,
                errors TEXT,
                counters TEXT,
                infos TEXT,
                address TEXT
            );
        """,
        "srbx_state_data": f"""
            CREATE TABLE IF NOT EXISTS {schema}.srbx_state_data (
                device_type TEXT,
                serial_number TEXT,
                last_heartbeat TEXT,
                first_heartbeat TEXT,
                timezone TEXT,
                install_date TEXT,
                store_id TEXT,
                gm_serial_number TEXT,
                gm_last_heartbeat TEXT,
                gm_mac TEXT,
                gm_sphere_id TEXT,
                gm_eth_status TEXT,
                kccm_version TEXT,
                qts_version TEXT,
                srb_version TEXT,
                menu_name TEXT,
                version_update_time TEXT,
                filter_cycles TEXT,
                door_cycles TEXT,
                heater_on_time TEXT,
                oven_on_time TEXT,
                left_mag_on_time TEXT,
                right_mag_on_time TEXT,
                total_cook_count TEXT,
                counter_update_time TEXT,
                commissioning_date TEXT,
                total_products_today TEXT,
                total_errors_today TEXT,
                address TEXT,
                date DATE
            );
        """,
        "srbx_twin_data": f"""
            CREATE TABLE IF NOT EXISTS {schema}.srbx_twin_data (
                address TEXT,
                date DATE,
                PRIMARY KEY (address, date)
            );
        """,
        "srbx_counters_data": f"""
            CREATE TABLE IF NOT EXISTS {schema}.srbx_counters_data (
                device_type TEXT,
                serial_number TEXT,
                counters_time TEXT,
                address TEXT,
                date DATE,
                counter TEXT
            );
        """
    }
    return table_schemas.get(table_name, "")

# Function to create SQLAlchemy engine
def get_engine():
    connection_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(connection_string) #, echo=True)
    return engine

def create_table_if_not_exists(engine, table_name, schema):
    try:
        with engine.connect() as connection:
            create_table_sql = get_create_table_sql(table_name, schema)
            if not create_table_sql:
                print(f"No SQL found for table: {table_name}")
                return

            connection.execute(text(create_table_sql))
            connection.commit()
            print(f"Table '{table_name}' created successfully")
    
    except SQLAlchemyError as e:
        print(f"Error creating table {table_name}: {e}")

def insert_into_db(data, table_name, SCHEMA_NAME=SCHEMA_NAME):
    engine = get_engine()
    if not engine:
        print("Failed to create SQLAlchemy engine")
        return
        
    create_table_if_not_exists(engine, table_name, SCHEMA_NAME)  
    
    try:
        with engine.connect() as connection:
            #print("entry")
            # Insert data
            metadata = MetaData()
            metadata.reflect(bind=engine)
            #print(metadata.tables)
            
            #Check if the table exists
            if table_name not in metadata.tables:
                raise ValueError(f"Table '{table_name}' does not exist in schema '{SCHEMA_NAME}'")

            else:
                #Ensure the table exists before attempting to insert data
                create_table_if_not_exists(engine, table_name, SCHEMA_NAME)

            # Access the reflected table
            #table = Table(table_name, metadata, schema=SCHEMA_NAME)
            table = Table(table_name, metadata, autoload_with=engine, schema=SCHEMA_NAME)
            
            insert_stmt = insert(table).values(data)
            
            # Print the insert statement for debugging
            #print(insert_stmt)
            
            #metadata.create_all(engine)
            # Use ON CONFLICT DO NOTHING
            on_conflict_stmt = insert_stmt.on_conflict_do_nothing()
            
            # Print the insert statement for debugging
            #print("Generated SQL:", on_conflict_stmt)
            
            # Execute the insert statement
            connection.execute(on_conflict_stmt)
            
            # Commit the transaction
            connection.commit()
            
            print(f"Data inserted into table '{table_name}'.")
            
    except NoSuchTableError as e:
        print(f"Table '{table_name}' does not exist in schema '{SCHEMA_NAME}'.")
    except SQLAlchemyError as e:
        # Handle SQLAlchemy errors
        print(f"Error inserting data into table '{table_name}': {e}")
        # Rollback the transaction in case of an error
        if 'connection' in locals():
            connection.rollback()
    except ValueError as e:
        # Handle specific errors like table not found
        print(e)
    except Exception as e:
        # Handle unexpected errors
        print(f"Unexpected error: {e}")

def process_errors(errors_data, file_date, address_content):
    try:
        #print("error entrer")
        errors_list = errors_data.get("errors", [])
        flattened_errors = [
            {
                "address": address_content,
                "date": file_date,
                "device_type": errors_data.get("device_type", ""),
                "serial_number": errors_data.get("serial_number", ""),
                "error_code": error.get("code", ""),
                "error_time": error.get("time", ""),
                "error_event": error.get("event", ""),
                "error_description": error.get("description", ""),
                "error_status": error.get("status", ""),
                "error_details": error.get("details", "").replace("{", "").replace("}", "").replace("\"", "")
            }
            for error in errors_list
        ]
        #print(flattened_errors)
        if flattened_errors:
            insert_into_db(flattened_errors, 'srbx_errors_data')

    except Exception as e:
        print(f"Error processing 'errors' section: {e}")

def process_products(products_data, file_date, address_content):
    try:
        results_list = products_data.get("results", [])
        flattened_products = [
            {
                "device_type": products_data.get("device_type", ""),
                "serial_number": products_data.get("serial_number", ""),
                "product_type": products_data.get("product_type", ""),
                "results_time": result.get("time", ""),
                "results_recipe_name": result.get("recipe_name", ""),
                "results_status": result.get("status", ""),
                "address": address_content,
                "date": file_date,
            }
            for result in results_list
        ]
        #print(flattened_products)
        #print(flattened_products)
        if flattened_products:
            insert_into_db(flattened_products, 'srbx_products_data')
    except Exception as e:
        print(f"Error processing 'products' section: {e}")

def process_metrics(metrics_data, file_date, address_content):
    try:
        metrics_entries = metrics_data.get("metrics", {})
        flattened_metrics = [
            {
                "device_type": metrics_data.get("device_type", ""),
                "serial_number": metrics_data.get("serial_number", ""),
                "date": date,
                "total_products": metrics.get("total_products", ""),
                "total_errors": metrics.get("total_errors", ""),
                "address": address_content
            }
            for date, metrics in metrics_entries.items()
        ]
        if flattened_metrics:
            insert_into_db(flattened_metrics, 'srbx_metrics_data')

    except Exception as e:
        print(f"Error processing 'metrics' section: {e}")

def process_counts(counts_data, file_date, address_content):
    try:
        counts_entries = counts_data.get("counts", {})
        flattened_counts = [
            {
                "device_type": counts_data.get("device_type", ""),
                "serial_number": serial,
                "date": date,
                "device_heartbeats": counts.get("device_heartbeats", ""),
                "gm_heartbeats": counts.get("gm_heartbeats", ""),
                "products": counts.get("products", ""),
                "errors": counts.get("errors", ""),
                "counters": counts.get("counters", ""),
                "infos": counts.get("infos", ""),
                "address": address_content,
            }
            for serial, dates in counts_entries.items()
            for date, counts in dates.items()
        ]
        if flattened_counts:
            insert_into_db(flattened_counts, 'srbx_counts_data')
    except Exception as e:
        print(f"Error processing 'counts' section: {e}")

def process_state(state_data, file_date, address_content):
    try:
        gm_data = state_data.get("gm", {})
        flattened_state = {
            "device_type": state_data.get("device_type", ""),
            "serial_number": state_data.get("serial_number", ""),
            "last_heartbeat": state_data.get("last_heartbeat", ""),
            "first_heartbeat": state_data.get("first_heartbeat", ""),
            "timezone": state_data.get("timezone", ""),
            "install_date": state_data.get("install_date", ""),
            "store_id": state_data.get("store_id", ""),
            "gm_serial_number": gm_data.get("serial_number", ""),
            "gm_last_heartbeat": gm_data.get("last_heartbeat", ""),
            "gm_mac": gm_data.get("mac", ""),
            "gm_sphere_id": gm_data.get("sphere_id", ""),
            "gm_eth_status": gm_data.get("eth_status", ""),
            "kccm_version": state_data.get("kccm_version", ""),
            "qts_version": state_data.get("qts_version", ""),
            "srb_version": state_data.get("srb_version", ""),
            "menu_name": state_data.get("menu_name", ""),
            "version_update_time": state_data.get("version_update_time", ""),
            "filter_cycles": state_data.get("filter_cycles", ""),
            "door_cycles": state_data.get("door_cycles", ""),
            "heater_on_time": state_data.get("heater_on_time", ""),
            "oven_on_time": state_data.get("oven_on_time", ""),
            "left_mag_on_time": state_data.get("left_mag_on_time", ""),
            "right_mag_on_time": state_data.get("right_mag_on_time", ""),
            "total_cook_count": state_data.get("total_cook_count", ""),
            "counter_update_time": state_data.get("counter_update_time", ""),
            "commissioning_date": state_data.get("commissioning_date", ""),
            "total_products_today": state_data.get("total_products_today", ""),
            "total_errors_today": state_data.get("total_errors_today", ""),
            "address": address_content,
            "date": file_date,
        }
        if flattened_state:
            insert_into_db([flattened_state], 'srbx_state_data')

    except Exception as e:
        print(f"Error processing 'state' section: {e}")

def process_twin(twin_data, file_date, address_content):
    try:
        def flatten_twin(data, parent_key='', sep='_'):
            items = []
            for k, v in data.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                if isinstance(v, dict):
                    items.extend(flatten_twin(v, new_key, sep=sep).items())
                else:
                    items.append((new_key, v))
            return dict(items)

        flattened_twin = flatten_twin(twin_data)
        flattened_twin["address"] = address_content
        flattened_twin["date"] = file_date
        twin_data_list.append(flattened_twin)
        
        if twin_data_list:
            insert_into_db(twin_data_list, 'srbx_twin_data')

    except Exception as e:
        print(f"Error processing 'twin' section: {e}")

def process_counters(counters_data, file_date, address_content):
    try:
        counters_list = counters_data.get("counters", [])
        flattened_counters = [
            {
                "device_type": counters_data.get("device_type", ""),
                "serial_number": counters_data.get("serial_number", ""),
                "counter_filter_cycles": counter.get("filter_cycles", ""),
                "counter_door_cycles": counter.get("door_cycles", ""),
                "counter_heater_on_time": counter.get("heater_on_time", ""),
                "counter_oven_on_time": counter.get("oven_on_time", ""),
                "counter_left_mag_on_time": counter.get("left_mag_on_time", ""),
                "counter_right_mag_on_time": counter.get("right_mag_on_time", ""),
                "counter_total_cook_count": counter.get("total_cook_count", ""),
                "counter_time": counter.get("time", ""),
                "address": address_content,
                "date": file_date,
            }
            for counter in counters_list
        ]
        if flattened_counters:
            insert_into_db(flattened_counters, 'srbx_counters_data')

    except Exception as e:
        print(f"Error processing 'counters' section: {e}")

def log_json_errors_to_file():
    try:
        if json_error_logs:
            with open("json_decoding_errors.txt", 'w') as log_file:
                log_file.write("JSON Decoding Errors:\n")
                log_file.write("=====================\n\n")
                for error in json_error_logs:
                    log_file.write(f"{error}\n\n")
    except Exception as e:
        print(f"Error logging JSON errors to file: {e}")

def log_most_recent_file():
    try:
        most_recent_date = max(file_dates, default=None, key=lambda x: datetime.strptime(x, '%Y-%m-%d'))
        if most_recent_date:
            df = pd.DataFrame({"Date": [most_recent_date]})
            df.to_csv("most_recent_date.csv", index=False)
    except Exception as e:
        print(f"Error logging most recent file date: {e}")

def log_file_dates_to_file():
    try:
        if file_dates:
            with open("file_dates.txt", 'w') as dates_file:
                for date in sorted(file_dates):
                    dates_file.write(f"{date}\n")
    except Exception as e:
        print(f"Error logging file dates to file: {e}")

#         print(f"Error searching and processing directory {directory}: {e}")

# Function to process each file
def process_file(file_path, file_date, address_content):
    # global address_content
    try:
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                data = file.read()
        except UnicodeDecodeError as e:
            print(f"UnicodeDecodeError for file {file_path}: {e}")
            try:
                with open(file_path, 'r', encoding='latin-1') as file:
                    data = file.read()
            except UnicodeDecodeError as e:
                print(f"Second attempt UnicodeDecodeError for file {file_path}: {e}")
                return
            except Exception as e:
                print(f"Error opening file with latin-1 encoding {file_path}: {e}")
                return
        except Exception as e:
            print(f"Error opening file {file_path}: {e}")
            return

        try:
            json_data = json.loads(data)
        except json.JSONDecodeError as e:
            error_message = f"Error decoding JSON in file {file_path}: {e}"
            log_json_error(file_path, error_message)
            json_error_logs.append(error_message)
            return
        if "errors" in json_data:
            process_errors(json_data["errors"], file_date, address_content)
        # Extracting and processing the 'products' section
        if "products" in json_data:
            process_products(json_data["products"], file_date, address_content)

        # Extracting and processing the 'metrics' section
        if "metrics" in json_data:
            process_metrics(json_data["metrics"], file_date, address_content)

        # Extracting and processing the 'counts' section
        if "counts" in json_data:
            process_counts(json_data["counts"], file_date, address_content)

        # Extracting and processing the 'state' section
        if "state" in json_data:
            process_state(json_data["state"], file_date, address_content)

        # Extracting and processing the 'twin' section
        twin_data = json_data.get("twin", None)
        if twin_data:
            process_twin(twin_data, file_date, address_content)
        # Extracting and processing the 'counters' section
        if "counters" in json_data:
            process_counters(json_data["counters"], file_date, address_content)

    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

def search_address(directory, parent_address_content=""):
    try:
        address_content = parent_address_content
        address_file_path = os.path.join(directory, "address.txt")
    
        # Check if address.txt exists and read it
        if os.path.isfile(address_file_path):
            try:
                #address_content = ""
                with open(address_file_path, 'r') as address_file:
                    address_content = address_file.read().strip()
                    print(f"Address content from {address_file_path}: {address_content}")
            except Exception as e:
                print(f"Error reading address.txt in directory {directory}: {e}") 
        else:
            print(f"address.txt not found in directory {directory}. Using parent address: {parent_address_content}")
        return address_content
    
    except Exception as e:
        print(f"Error reading address.txt in directory {directory}: {e}")  
        return parent_address_content
        
# Recursive function to search for files and process them
def search_and_process(directory, parent_address_content=""):
    try:
        # Pattern to match the filename with date format
        pattern = re.compile(r'WBT-MC-\d{13}_(\d{4}-\d{2}-\d{2})\.txt')
        # Date threshold
        date_threshold = datetime.strptime("2021-11-01", '%Y-%m-%d')
        address_content = search_address(directory,parent_address_content="")
        
        for entry in os.listdir(directory):
            #print(entry)
            entry_path = os.path.join(directory, entry)
            
            if os.path.isdir(entry_path):
                search_and_process(entry_path, address_content)
            else:
                match = pattern.match(entry)
                if match:
                    date_str = match.group(1)
                    try:
                        file_date = datetime.strptime(date_str, '%Y-%m-%d')
                        if file_date >= date_threshold:
                            file_dates.append(date_str)
                            process_file(entry_path, date_str, address_content)
                    except ValueError:
                        continue
    except Exception as e:
        print(f"Error searching and processing directory {directory}: {e}")


if __name__ == "__main__":
    try:
        print("Entering the script...")
        start_directory = r"R:\K_Archive_\Starbucks SRC\SRBX_DATA_EXTRACTS\GARY_SRBX_EXTRACT_SAMPLE"
        if not os.path.exists(start_directory):
            print(f"Error: The directory {start_directory} does not exist.")
        else:
            print(f"Starting directory walk in: {start_directory}")
            try :
                # Begin the recursive search and processing
                search_and_process(start_directory)
                # log_json_errors_to_file()
                # log_most_recent_file()
                # log_file_dates_to_file()
                #print("Script completed successfully.")
            except Exception as e:
                print("error in search and process func :", e)
        
    except Exception as e:
        print(f"Error in the script: {e}")
