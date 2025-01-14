NUMBER_OF_DATA = [1000, 10000, 100000, 1000000]
BATCH_SIZE = [1000, 10000, 100000, 100000]
DB_HOST = "localhost"
DB_USER = "user"
DB_PASS = "password"
DB_NAME = "dormitory_management_system"

SILENT_MODE = False

PATH_TO_LOGS = 'output/logs/'
PATH_TO_STATS = 'output/stats/'

CONSOLE_PROMPT = "$console> "

WRONG_COMMAND = f"{CONSOLE_PROMPT}[ERROR] Unknown command"
NOT_IMPLEMENTED = f"{CONSOLE_PROMPT}[ERROR] Command not implemented"
CLOSE_APP = f"{CONSOLE_PROMPT}[INFO] Application is closing..."

PROGRESS_BAR_LENGTH = 50
SELECT_MSG = ["Group reservation per device", "Count average reservation time per device",
              "Count number of admins", "Find problem by id", "Find dormitory messages",
              "Find reservations earlier then concrete data", "Find users",
              "Count problems by status", "Group messages per dormitory",
              "Completed"]
DELETE_MSG = ["Many not in batch", "Many in batch", "One dormitory", "Completed"]
UPDATE_MSG = ["Password and other fields when user is admin", "Reservation time",
              "Status when problem is older than", "One message", "Completed"]
INSERT_DORM = ["Insert dormitories", "Completed"]
INSERT_MSG = ["Create users", "Create devices", "Insert problems",
              "Insert messages", "Insert reservations", "Completed"]

MAX_WORD_LEN = max(len(i) for i in SELECT_MSG + DELETE_MSG + UPDATE_MSG + INSERT_MSG + INSERT_DORM)