import os
import logging


class HoldingsLogger:
    def __init__(self, log_name, log_dir=os.path.join(os.getcwd(), "logs")):
        self.log_dir = log_dir
        self.log_name = log_name
        self.filename = os.path.join(self.log_dir, self.log_name)
        if not os.path.isdir(self.log_dir):
            os.mkdir(self.log_dir)
        logging.basicConfig(level=logging.DEBUG, filename=self.filename, filemode='w')