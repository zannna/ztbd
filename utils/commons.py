number_of_data = [1000, 10000, 100000, 1000000]
DB_HOST = "localhost"
DB_USER = "user"
DB_PASS = "password"
DB_NAME = "dormitory_management_system"

PATH_TO_LOGS = 'output/logs/'
PATH_TO_STATS = 'output/stats/'

CONSOLE_PROMPT = "$console> "
MYSQL_PROMPT = "$mysql> "
MONGO_PROMPT = "$mongo> "

WRONG_COMMAND = lambda p : f"{p}[ERROR] Unknown command"
NOT_IMPLEMENTED = lambda p : f"{p}[ERROR] Command not implemented"
CLOSE_APP = lambda p : f"{p}[INFO] Application is closing..."
DATABASE_NOT_EXIST = lambda p : f"{p}[ERROR] Database does not exist. Exiting..."

CREATE_DATABASE = "Database creation ([q] to quit)"
DEFAULT_SIZE = 1000
ELEMENTS_COUNT = f"Number of elements to generate in tables (default {DEFAULT_SIZE}): "
BATCH_SIZE = f"Enter size of batch (default {DEFAULT_SIZE}): "
INVALID_VALUE = "[ERROR] Invalid value"
BATCH_SIZE_INFO = "[INFO] Batch size > number of elements -> using default value"
VALUE_TOO_LOW = "value too small (must be greater than 1000)"
VALUE_TOO_BIG = "value too big (must be less than 1000000)"