from data.generation import Generator
from data.graph import Grapher
from tasks.delete_task import DeleteTask
from tasks.select_task import SelectTask
from tasks.update_task import UpdateTask
from utils.commons import (CONSOLE_PROMPT, WRONG_COMMAND,
                           NUMBER_OF_DATA, BATCH_SIZE)
from utils.logger import Logger


def print_help(command : str)-> None:
    if command == "c":
        print("")
        print("  [c]   compare databases")
        print("  params:")
        print("    <n> number of iterations that will be performed")
        print("")
    elif command == "l":
        print("")
        print("  [l]   show program logs")
        print("")
    elif command == "q":
        print("")
        print("  [q]   quit application")
        print("")
    elif command == "h":
        print("")
        print("  [h]   display available commands")
        print("  params:")
        print("    <cmd> show command help")
        print("")
    else:
        print(WRONG_COMMAND)


def main_menu(cmp : bool) -> None:
    print("Database comparison tool v1.0")
    print("Usage: <command> <optional params>")
    print("Available commands: ")
    print("  [c]       compare databases")
    if cmp:
        print("  [g]       create comparison graphs")
    print("  [l]       show program logs")
    print("  [h]       display available commands")
    print("  [q]       quit application")
    print("")


def compare(iters : int) -> bool:
    Logger.clear_stats()
    Logger.log("INFO", "Comparing databases - start")
    print("[INFO] Comparing databases - start")
    for index, num_elements in enumerate(NUMBER_OF_DATA):
        batch_size = BATCH_SIZE[index]
        Logger.console(f"[INFO] Number of data: {num_elements}")
        print(f"[INFO] Number of data: {num_elements}")
        insert_task = Generator(num_elements, batch_size)
        select_task = SelectTask(num_elements)
        update_task = UpdateTask(num_elements)
        delete_task = DeleteTask(num_elements)

        for i in range(iters):
            Logger.console(f"[INFO] Iteration {i + 1} / {iters}")
            print(f"[INFO] Iteration {i + 1} / {iters}")
            insert_task.run()
            select_task.run()
            update_task.run()
            delete_task.run()
    Logger.log("INFO", "Comparing databases - finished")
    print("[INFO] Comparing databases - finished")
    return True

if __name__ == '__main__':
    Logger.log_file("INFO", "Program started")
    compared = False
    print()
    main_menu(compared)
    while True:
        op = input(CONSOLE_PROMPT).lower()
        tmp = op.split(" ")
        if len(tmp) == 2:
            op, opt = tmp[0].strip(), tmp[1].strip()
        else:
            op = tmp[0].strip()
            opt = ""
        if op == "q":
            print("[INFO] Application is closing...")
            Logger.log_file("INFO", "Program closed")
            break
        if op == "c":
            try:
                opt = int(opt)
            except ValueError:
                Logger.console("[INFO] Using default value (1)")
                opt = 1
            compared = compare(opt)
        elif op == "g":
            if not compared:
                print(f"{CONSOLE_PROMPT}[ERROR] Databases were not compared")
                continue
            grapher = Grapher()
            if grapher.create():
                print("[INFO] Graphs were created")
            else:
                print(f"{CONSOLE_PROMPT}[ERROR] Stats files not found")
        elif op == "l":
            Logger.print_logs()
        elif op == "h":
            if opt == "":
                main_menu(compared)
            else:
                print_help(opt)
        elif op == "":
            continue
        else:
            print(WRONG_COMMAND)