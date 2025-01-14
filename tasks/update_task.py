from math import floor

from data.stats_collector import StatsCollector
from database.queries.mongo.updateMongo import UpdateMongo
from database.queries.mysql.updateSQL import UpdateSQL
from tasks.task import Task
from utils.commons import PROGRESS_BAR_LENGTH, UPDATE_MSG
from utils.logger import Logger


class UpdateTask(Task):
    def __init__(self):
        super().__init__()
        self.operation = "UPDATE"
        self.mysql_stats = StatsCollector("mysql", self.operation, 0, 0)
        self.mongo_stats = StatsCollector("mongo", self.operation, 0, 0)
        self.messages = UPDATE_MSG
        self.progress_add = int(floor(PROGRESS_BAR_LENGTH / len(self.messages)))

    def run(self) -> None:
        if not self.connect():
            self.connect()

        Logger.log("INFO", "Performing UPDATE operations")
        self.progress = 0
        self.index = 0

        # MYSQL
        self.progress_bar()
        result = UpdateSQL.update_password_and_other_fields_when_user_is_admin(self.connection)
        self.mysql_stats.add_stats(result["total_time"], result["affected_rows"])
        # MONGO
        self.progress_bar(1)
        result, mongo_time = UpdateMongo.update_password_and_other_fields_when_user_is_admin(self.database)
        self.mongo_stats.add_stats(mongo_time, result.modified_count)

        # MYSQL
        self.progress_bar(2)
        result = UpdateSQL.update_reservation_time(self.connection)
        self.mysql_stats.add_stats(result["total_time"], result["affected_rows"])
        # MONGO
        self.progress_bar(1)
        result, mongo_time = UpdateMongo.update_reservation_time(self.database)
        self.mongo_stats.add_stats(mongo_time, result.modified_count)

        # MYSQL
        self.progress_bar(2)
        result = UpdateSQL.update_status_when_problem_is_older_than(self.connection)
        self.mysql_stats.add_stats(result["total_time"], result["affected_rows"])
        # MONGO
        self.progress_bar(1)
        result, mongo_time = UpdateMongo.update_status_when_problem_is_older_than(self.database)
        self.mongo_stats.add_stats(mongo_time, result.modified_count)

        # MYSQL
        self.progress_bar(2)
        result = UpdateSQL.update_one_message(self.connection)
        self.mysql_stats.add_stats(result["total_time"], result["affected_rows"])
        # MONGO
        self.progress_bar(1)
        result, mongo_time = UpdateMongo.update_one_message(self.database)
        self.mongo_stats.add_stats(mongo_time, result.modified_count)

        self.connection.close()

        Logger.save_stats(self.mysql_stats)
        Logger.save_stats(self.mongo_stats)
        self.progress_bar(3)
        Logger.log("INFO", f"MySQL - data updated in {self.mysql_stats.total_time:.02f} seconds.")
        Logger.log("INFO", f"Mongo - data updated in {self.mongo_stats.total_time:.02f} seconds.")