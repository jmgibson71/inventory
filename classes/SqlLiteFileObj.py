import classes.SampleHash
from databases import SqlliteGen as sql
import logging


class FileMap:
    def __init__(self, path):
        """
        :type path : PTH
        :param path:
        """
        self.file_name = path.name
        self.file_path = str(path)
        self.hash = None
        self.id = 0

    def hash_file(self):
        smpl = classes.SampleHash.SampleHash(self.file_path)
        self.hash = smpl.hash()

    def insert_into_db(self, file):
        conn = sql.get_connection(file)
        c = conn.cursor()
        c.execute("""UPDATE inv_rough SET file_hash=? where file_path=?""", (self.hash, self.file_path))
        c.close()
        conn.close()

    def first_pass_insert(self, file):
        conn = sql.get_connection(file)
        c = conn.cursor()
        c.execute("""INSERT into inv_rough ('file_name', 'file_path') values (?,?)""", (self.file_name, self.file_path))
        c.close()
        conn.close()

    def to_string(self, id=None):
        if id is None:
            print('{hsh}={fn}'.format(hsh=self.hash, fn=self.file_path.encode("utf-8")))
        else:
            print('{id}={fn}'.format(id=id, fn=self.file_path.encode("utf-8")))
