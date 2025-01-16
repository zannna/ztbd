import os

from data.stats_collector import StatsCollector
from utils.commons import PATH_TO_LOGS, SILENT_MODE, PATH_TO_STATS
from datetime import datetime

class Logger:
    @staticmethod
    def console(msg : str) -> None:
        if not SILENT_MODE:
            print(msg)

    @staticmethod
    def log_file(msg_type : str, msg : str) -> None:
        now = datetime.now()
        message_file = f"[{msg_type} - {now.strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
        filename = PATH_TO_LOGS + now.strftime('%Y-%m-%d') + ".txt"
        if os.path.exists(filename):
            mode = 'a'
        else:
            mode = 'w'

        with open(filename, mode) as f:
            f.write(message_file + '\n')

    @staticmethod
    def log(msg_type : str, msg : str) -> None:
        message_console = f"[{msg_type}] {msg}"
        Logger.console(message_console)
        Logger.log_file(msg_type, msg)

    @staticmethod
    def print_logs() -> None:
        now = datetime.now()
        filename = PATH_TO_LOGS + now.strftime('%Y-%m-%d') + ".txt"

        with open(filename, "r") as f:
            lines = f.readlines()

        print(f"[LOGS] - {filename}")
        for line in lines:
            print(line, end="")

    @staticmethod
    def save_stats(stats : StatsCollector) -> None:
        if not os.path.exists(stats.filepath):
           with open(stats.filepath, "w") as f:
                f.write("Operation,Elements,BatchSize,TotalElements,TotalTime,TimePerRecord,"
                        "TimePerBatch,TimePerTable,Function\n")

        with open(stats.filepath, "a+") as f:
            f.write(stats.print_stats())

    @staticmethod
    def clear_stats():
        f1 = PATH_TO_STATS + 'mongo.csv'
        f2 = PATH_TO_STATS + 'mysql.csv'
        if os.path.exists(f1):
            os.remove(f1)
        if os.path.exists(f2):
            os.remove(f2)