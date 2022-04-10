from ef_exercise_functions import *

import logging
import logging.handlers

file_handler = logging.FileHandler(
    './log_files/logs.log')
file_format = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_format)

file_logger = logging.getLogger('file_logger')
file_logger.addHandler(file_handler)
file_logger.setLevel(logging.DEBUG)
file_logger.propagate = False

date = str(input("Input date in yyyymmdd form: "))
exercise = int(input("Input exercise number: "))

def main(exercise, date):
    if exercise == 1:
        print(exercice_one(date))
    if exercise == 2:
        print(exercice_two(date))

        
if __name__ == "__main__":

    try:
        file_logger.info("Function Started")
        main(exercise, date)
        file_logger.info("Function Finished Successfully")
    except Exception as e:
        file_logger.exception(e)
        raise(e)
