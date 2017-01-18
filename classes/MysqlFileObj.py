import os


class FileMap:
    def __init__(self, path):
        """
        :type path : str
        :param path:
        """
        self.file_name = path.split(os.path.sep)[-1]
        self.file_path = str(path)
        self.file_size = os.lstat(self.file_path).st_size

