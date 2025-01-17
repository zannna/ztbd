import os
import matplotlib.pyplot as plt
import pandas as pd

from utils.commons import PATH_TO_STATS, PATH_TO_GRAPHS, SELECT_MSG, UPDATE_MSG, DELETE_MSG, BAR_CHART_FUNCS, \
    NUMBER_OF_DATA, DORMITORY_MULT


class Grapher:
    def __init__(self):
        self.path = PATH_TO_GRAPHS
        self.mysql_dfs = None
        self.mongo_dfs = None
        self.mongo_df = None
        self.mysql_df = None

        if not os.path.exists(self.path):
            os.mkdir(self.path)

        file_tmp = PATH_TO_STATS
        if os.path.exists(file_tmp):
            self.mongo_df = pd.read_csv(file_tmp + 'mongo.csv')
            self.mysql_df = pd.read_csv(file_tmp + 'mysql.csv')


    def create(self) -> bool:
        if self.mongo_df is None or self.mysql_df is None:
            return False

        print("[INFO] Generating graphs...")
        mysql_df = self.mysql_df.sort_values(by="Operation")
        mongo_df = self.mongo_df.sort_values(by="Operation")

        ops = ['INSERT', 'SELECT', 'UPDATE', 'DELETE']

        self.mongo_dfs = {op : mongo_df[mongo_df["Operation"] == op] for op in ops}
        self.mysql_dfs = {op : mysql_df[mysql_df["Operation"] == op] for op in ops}

        c = ['Operation', 'Elements', 'TotalElements', 'TotalTime', 'TimePerRecord', 'Function']
        self.create_graphs('INSERT')
        self.create_graphs('SELECT', c, SELECT_MSG[:-1])
        self.create_graphs('UPDATE', c, UPDATE_MSG[:-1])
        self.create_graphs('DELETE', c, DELETE_MSG[:-2])

        return True


    def create_graphs(self, op : str, cols : list = None, fs : list = None):
        mongo_all = self.mongo_dfs[op]
        mongo_all = mongo_all[mongo_all["Operation"] == op]
        if cols:
            mongo_all = mongo_all[cols]

        mysql_all = self.mysql_dfs[op]
        mysql_all = mysql_all[mysql_all["Operation"] == op]
        if cols:
            mysql_all = mysql_all[cols]

        self.create_comparison_graph(op, mysql_all, mongo_all, "All")

        if fs:
            for f in fs:
                f_name = f"{op} {f.lower()}"
                self.create_comparison_graph(op, mysql_all, mongo_all, f_name)
                if f in BAR_CHART_FUNCS:
                    self.create_bar_chart(mysql_all, mongo_all, f_name)


    def create_comparison_graph(self, op, mysql_data, mongo_data, f):
        a = mysql_data[mysql_data['Function'] == f]
        b = mongo_data[mongo_data['Function'] == f]

        f = f"{op} {f.lower()}" if f == "All" else f

        a = a.groupby('Elements', as_index=False).agg({
            'TotalTime': 'mean',
            'TotalElements': 'mean'
        })

        b = b.groupby('Elements', as_index=False).agg({
            'TotalTime': 'mean',
            'TotalElements': 'mean'
        })

        plt.figure(figsize=(10, 6))

        plt.plot(a['TotalElements'], a['TotalTime'], label='MySQL', marker='o', linestyle='-')
        plt.plot(b['TotalElements'], b['TotalTime'], label='MongoDb', marker='o', linestyle='-')

        plt.title(f'{f}', fontsize=14)
        plt.xlabel('Number of elements', fontsize=12)
        plt.ylabel('Time (sec)', fontsize=12)
        plt.legend(fontsize=12)
        plt.grid(True)

        plt.tight_layout()
        #plt.show()
        plt.savefig(self.path + f.replace(" ", "_") + '.jpg', format="jpg", dpi=300)
        plt.close()


    def create_bar_chart(self, mysql_data, mongo_data, f : str):
        a = mysql_data[mysql_data['Function'] == f]
        b = mongo_data[mongo_data['Function'] == f]

        x = NUMBER_OF_DATA[-1]
        a = a[a['Elements'] == x]
        b = b[b['Elements'] == x]

        a = a['TotalTime'].mean()
        b = b['TotalTime'].mean()

        dbs = ['MySQL', 'MongoDb']
        avg = [a, b]

        plt.figure(figsize=(8, 6))
        plt.bar(dbs, avg, color=['blue', 'red'], alpha=0.7)
        for i, value in enumerate(avg):
            plt.text(i, value, f'{value:.5f}', ha='center', fontsize=10, color='black')

        plt.title(f'{f}', fontsize=14)
        plt.ylabel('Average time (sec)', fontsize=12)
        plt.xlabel('Database', fontsize=12)
        plt.grid(axis='y', linestyle='--', alpha=0.7)

        plt.tight_layout()
        # plt.show()
        plt.savefig(self.path + f.replace(" ", "_") + '_bar.jpg', format="jpg", dpi=300)
        plt.close()