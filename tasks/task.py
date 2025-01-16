import mysql.connector
from abc import abstractmethod
from pymongo import MongoClient

from data.stats_collector import StatsCollector
from utils.commons import DB_HOST, DB_USER, DB_PASS, DB_NAME, PROGRESS_BAR_LENGTH, MAX_WORD_LEN, SILENT_MODE
from utils.logger import Logger


class Task:
    def __init__(self):
        self.connection = None
        self.client = None
        self.database = None
        self.connected = False
        self.operation = None
        self.mongo_stats = None
        self.mysql_stats = None
        self.messages = []
        self.progress_add = 0
        self.progress = 0
        self.index = 0
        self.elements = 0
        self.max_len = MAX_WORD_LEN


    def reset(self) -> None:
        """
        Reset task before next iteration
        :return: None
        """
        self.mongo_stats.reset()
        self.mysql_stats.reset()
        self.progress = 0
        self.index = 0


    def progress_bar(self, op : int = 0) -> None:
        """
        Print progress bar with message
        :param op define which variable to increment 0 - none / 1 - progress / 2 - index / 3 - both
        :return: None
        """
        if op == 0:
            pass
        elif op == 1:
            self.progress += self.progress_add
        elif op == 2:
            self.index += 1
        elif op == 3:
            self.index = len(self.messages) - 1
            self.progress = PROGRESS_BAR_LENGTH
        else:
            return

        if SILENT_MODE:
            return

        word = self.messages[self.index]
        spaces = ' ' * (self.max_len - len(word))
        msg = f"[{self.operation}] {word}{spaces}"
        if self.progress == PROGRESS_BAR_LENGTH:
            print(f"\r{msg}", end="")
            print("")
        else:
            print(f"\r{msg} [" + "#" * self.progress + " " * (PROGRESS_BAR_LENGTH - self.progress) + "]", end="")


    def connect(self) -> bool:
        """
        Perform connection to database
        :return: True if connection was successful
        """
        self.connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME
        )
        self.client = MongoClient("mongodb://admin:password@localhost:27017/")
        self.database = self.client[DB_NAME]
        return True


    def disconnect(self) -> None:
        """
        Disconnect from database
        :return: None
        """
        self.client.close()
        self.connection.close()
        self.connected = False


    def get_function(self) -> str:
        """
        Return name of the function performing at the moment
        :return: name of the function
        """
        return self.messages[self.index]


    def save_function_stats(self, db : str, time : float, records : int) -> None:
        """
        Save function stats to file (only for UPDATE / SELECT / DELETE)
        :param db: name of the database
        :param time: time of execution
        :param records: number of records modified / selected
        :return: None
        """
        f = self.operation + " " + self.get_function().lower()
        stats_tmp = StatsCollector(db, self.operation, self.elements, 0, f)
        stats_tmp.add_stats(time, records)
        Logger.save_stats(stats_tmp)


    @abstractmethod
    def run(self) -> None:
        """
        Perform task in database
        :return: None
        """
        pass