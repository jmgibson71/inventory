import os
import logging


class HoldingsLogger:
    def __init__(self, log_name, mode='w', log_dir=os.path.join(os.getcwd(), "logs")):
        self.log_dir = log_dir
        self.log_name = log_name
        self.filename = os.path.join(self.log_dir, self.log_name)
        if not os.path.isdir(self.log_dir):
            os.mkdir(self.log_dir)
        logging.basicConfig(level=logging.DEBUG, filename=self.filename, filemode=mode)

    def get_log_dir(self):
        return self.log_dir


class ReportHandler:
    def __init__(self, rep_name, rep_dir=os.path.join(os.getcwd(), "reports")):
        self.report_name = rep_name
        self.report_dir = rep_dir
        self.filename = os.path.join(self.report_dir, self.report_name)
        if not os.path.isdir(self.report_dir):
            os.mkdir(self.report_dir)

    def get_file_handle(self, mode):
        return open(self.filename, mode)
