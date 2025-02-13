# constants.py

import re
from datetime import datetime

# Pattern to match the filename with date format
PATTERN = re.compile(r'WBT-MC-\d{13}_(\d{4}-\d{2}-\d{2})\.txt')

# Date threshold
DATE_THRESHOLD = datetime.strptime("2021-11-01", '%Y-%m-%d')

# Keys
KEY_ERRORS = "errors"
KEY_DEVICE_TYPE = "device_type"
KEY_SERIAL_NUMBER = "serial_number"
KEY_ERROR_CODE = "code"
KEY_ERROR_TIME = "time"
KEY_ERROR_EVENT = "event"
KEY_ERROR_DESCRIPTION = "description"
KEY_ERROR_STATUS = "status"
KEY_ERROR_DETAILS = "details"
KEY_PRODUCTS = "products"
KEY_RESULTS = "results"
KEY_RESULTS_TIME = "time"
KEY_RESULTS_RECIPE_NAME = "recipe_name"
KEY_RESULTS_STATUS = "status"
KEY_METRICS = "metrics"
KEY_TOTAL_PRODUCTS = "total_products"
KEY_TOTAL_ERRORS = "total_errors"
KEY_COUNTS = "counts"
KEY_DEVICE_HEARTBEATS = "device_heartbeats"
KEY_GM_HEARTBEATS = "gm_heartbeats"
KEY_PRODUCTS_COUNT = "products"
KEY_ERRORS_COUNT = "errors"
KEY_COUNTERS = "counters"
KEY_INFOS = "infos"
KEY_STATE = "state"
KEY_GM = "gm"
KEY_LAST_HEARTBEAT = "last_heartbeat"
KEY_FIRST_HEARTBEAT = "first_heartbeat"
KEY_TIMEZONE = "timezone"
KEY_INSTALL_DATE = "install_date"
KEY_STORE_ID = "store_id"
KEY_GM_SERIAL_NUMBER = "serial_number"
KEY_GM_LAST_HEARTBEAT = "last_heartbeat"
KEY_GM_MAC = "mac"
KEY_GM_SPHERE_ID = "sphere_id"
KEY_GM_ETH_STATUS = "eth_status"
KEY_KCCM_VERSION = "kccm_version"
KEY_QTS_VERSION = "qts_version"
KEY_SRB_VERSION = "srb_version"
KEY_MENU_NAME = "menu_name"
KEY_VERSION_UPDATE_TIME = "version_update_time"
KEY_FILTER_CYCLES = "filter_cycles"
KEY_DOOR_CYCLES = "door_cycles"
KEY_HEATER_ON_TIME = "heater_on_time"
KEY_OVEN_ON_TIME = "oven_on_time"
KEY_LEFT_MAG_ON_TIME = "left_mag_on_time"
KEY_RIGHT_MAG_ON_TIME = "right_mag_on_time"
KEY_TOTAL_COOK_COUNT = "total_cook_count"
KEY_COUNTER_UPDATE_TIME = "counter_update_time"
KEY_COMMISSIONING_DATE = "commissioning_date"
KEY_TOTAL_PRODUCTS_TODAY = "total_products_today"
KEY_TOTAL_ERRORS_TODAY = "total_errors_today"
KEY_TWIN = "twin"
KEY_ADDRESS = "address"
KEY_DATE = "date"
KEY_HISTORY = "history"

# Initialize lists to store data for each section
ERRORS_DATA_LIST = []
PRODUCTS_DATA_LIST = []
METRICS_DATA_LIST = []
HISTORY_DATA_LIST = []
COUNTS_DATA_LIST = []
STATE_DATA_LIST = []
TWIN_DATA_LIST = []
COUNTERS_DATA_LIST = []

# Global variable to store last known address content
ADDRESS_CONTENT = ""

# List to store JSON decoding errors
JSON_ERROR_LOGS = []
FILE_DATES = []
