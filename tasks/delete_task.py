from math import floor

from data.stats_collector import StatsCollector
from database.queries.mongo.deleteMongo import DeleteMongo
from database.queries.mysql.deleteSQL import DeleteSQL
from utils.commons import DELETE_MSG, PROGRESS_BAR_LENGTH
from utils.logger import Logger
from tasks.task import Task

class DeleteTask(Task):
    def __init__(self):
        super().__init__()
        self.operation = "DELETE"
        self.mysql_stats = StatsCollector("mysql", self.operation, 0, 0)
        self.mongo_stats = StatsCollector("mongo", self.operation, 0, 0)
        self.messages = DELETE_MSG
        self.progress_add = int(floor(PROGRESS_BAR_LENGTH / len(self.messages)))

    def run(self):
        if not self.connect():
            self.connect()

        self.progress = 0
        self.index = 0
        Logger.log("INFO", "Performing DELETE operations")

        # MYSQL
        dorm_names = [f"Dormitory {i}" for i in range(1, 10)]
        self.progress_bar()
        elapsed = DeleteSQL.delete_many_not_in_batch(self.connection, dorm_names)
        self.mysql_stats.add_stats(elapsed, len(dorm_names))
        # MONGO
        self.progress_bar(1)
        elapsed = DeleteMongo.delete_many_not_in_batch(self.database, dorm_names)
        self.mongo_stats.add_stats(elapsed, len(dorm_names))

        # MYSQL
        self.progress_bar(2)
        dorm_names = [f"Dormitory {i}" for i in range(10, 20)]
        placeholders = ', '.join(['%s'] * len(dorm_names))
        elapsed = DeleteSQL.delete_many_in_batch(self.connection, dorm_names, placeholders)
        self.mysql_stats.add_stats(elapsed, len(dorm_names))
        # MONGO
        self.progress_bar(1)
        elapsed = DeleteMongo.delete_many_in_batch(self.database, dorm_names)
        self.mongo_stats.add_stats(elapsed, len(dorm_names))

        # MYSQL
        dorm_name = f"Dormitory 3979"
        self.progress_bar(2)
        elapsed = DeleteSQL.delete_one(self.connection, dorm_name)
        self.mongo_stats.add_stats(elapsed, 1)
        # MONGO
        self.progress_bar(1)
        elapsed = DeleteMongo.delete_one(self.database, dorm_name)
        self.mongo_stats.add_stats(elapsed, 1)

        self.connection.close()
        self.progress_bar(3)
        Logger.log("INFO", f"MySQL - data deleted in {self.mysql_stats.total_time:.02f} seconds.")
        Logger.log("INFO", f"Mongo - data deleted in {self.mongo_stats.total_time:.02f} seconds.")
        Logger.save_stats(self.mongo_stats)
        Logger.save_stats(self.mysql_stats)