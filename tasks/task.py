import mysql.connector
from abc import abstractmethod
from pymongo import MongoClient

from utils.commons import DB_HOST, DB_USER, DB_PASS, DB_NAME, PROGRESS_BAR_LENGTH, MAX_WORD_LEN, SILENT_MODE


class Task:
    def __init__(self):
        self.connection = None
        self.client = None
        self.database = None
        self.connected = False
        self.operation = None
        self.messages = []
        self.progress_add = 0
        self.progress = 0
        self.index = 0
        self.max_len = MAX_WORD_LEN


    def progress_bar(self, op : int = 0) -> None:
        """
        Print progress bar with message
        :param op define which variable to increment 0 - none / 1 - progress / 2 - index / 3 - both
        :return: None
        """
        if SILENT_MODE:
            return

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


    @abstractmethod
    def run(self) -> None:
        """
        Perform task in database
        :return: None
        """
        pass