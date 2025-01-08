import os

from data.stats_collector import StatsCollector
from utils.commons import PATH_TO_LOGS
from datetime import datetime

class Logger:
    @staticmethod
    def log(msg_type : str, msg : str) -> None:
        now = datetime.now()
        filename = PATH_TO_LOGS + now.strftime('%Y-%m-%d') + ".txt"
        message = f"[{msg_type} - {now.strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
        print(message)

        if os.path.exists(filename):
            mode = 'a'
        else:
            mode = 'w'

        with open(filename, mode) as f:
            f.write(message + '\n')

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
                f.write("Operation,Elements,BatchSize,TotalElements,TotalTime,TimePerRecord,TimePerBatch,TimePerTable\n")

        with open(stats.filepath, "a+") as f:
            f.write(stats.print_stats())