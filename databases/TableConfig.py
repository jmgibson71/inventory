from pathlib import Path


class CreateMainTable:
    def __init__(self, file_name):
        self.db_file = file_name
        self.table_name = "inv_rough"
        self.col2 = 'file_name'
        self.col2_ft = 'TEXT'
        self.col3 = 'file_path'
        self.col3_ft = 'TEXT'
        self.col4 = 'file_hash'
        self.col4_ft = 'TEXT'

    def create(self):
        return 'CREATE VIRTUAL TABLE {tn} USING fts4({c2} {c2t}, {c3} {c3t},{c4} {c4t})'.format(
            tn=self.table_name,
            c2=self.col2,
            c2t=self.col2_ft,
            c3=self.col3,
            c3t=self.col3_ft,
            c4=self.col4,
            c4t=self.col4_ft)

    def main_table_created(self):
        if Path(self.db_file).is_file():
            return True
        return False


class CreateCompareTable:
    def __init__(self, file):
        self.db_file = file
        self.table_name = "inv_compare"
        self.col1 = "hash"
        self.col1_ft = "TEXT"
        self.col2 = "file_name"
        self.col2_ft = "TEXT"
        self.col3 = "collision"
        self.col3_ft = "INTEGER"

    def create(self):
        return 'CREATE TABLE {tn} ({c1} {c1t}, {c2} {c2t}, {c3} {c3t})'.format(
            tn=self.db_file,
            c1=self.col1,
            c1t=self.col1_ft,
            c2=self.col2,
            c2t=self.col2_ft,
            c3=self.col3,
            c3t=self.col3_ft)

    def table_created(self):
        f = Path(self.db_file)
        if f.is_file():
            return True
        return False


class CreateDirectoryHashTable:
    """
     CreateDirectoryHashTable creates a table to track fingerprint using a combined hash of the directories filenames
     and os.stat information to show that nothing new has been added to a subdirectory tree or modified within a
     subdirectory tree.
     NOTE: This is not a substitute for a full fixity check of the target systems. It is meant to provide a quick
     way to tell if changes have been made, and where.
    """
    def __init__(self, file):
        self.db_file = file
        self.table_name = 'quick_hash_table'
        self.directory_name = "directory_path"
        self.directory_hash = "directory_hash"
        self.last_checked = "last_checked"
        self.changed_since_last = "changed_since_last"

    def create(self):
        return 'CREATE TABLE {tn} USING fts4({dn}, {dh},{lc},{csl})'.format(
            tn=self.table_name,
            dn=self.directory_name,
            dh=self.directory_hash,
            lc=self.last_checked,
            csl=self.changed_since_last
        )

    def table_created(self):
        f = Path(self.db_file)
        if f.is_file():
            return True
        return False


class MysqlCreateMainTable:

    def __init__(self):

        self.table = (
            "CREATE TABLE `inv_rough` ("
            " `id` INT UNSIGNED AUTO_INCREMENT NOT NULL PRIMARY KEY,"
            " `file_name` TEXT(200),"
            " `file_path` TEXT(500),"
            " `file_size` BIGINT UNSIGNED,"
            " `file_hash` CHAR(32),"
            " `hashed` BOOLEAN NOT NULL DEFAULT 0) ENGINE=MyISAM"
        )

        self.table_idx = (
            "CREATE TABLE `inv_rough` ("
            " `id` INT UNSIGNED AUTO_INCREMENT NOT NULL PRIMARY KEY,"
            " `file_name` TEXT(200),"
            " `file_path` TEXT(500),"
            " `file_size` BIGINT UNSIGNED,"
            " `file_hash` CHAR(32),"
            " `hashed` BOOLEAN NOT NULL DEFAULT 0,"
            " FULLTEXT idx1 (`file_path`),"
            " FULLTEXT idx2 (`file_hash`)) ENGINE=MyISAM"
        )



    def create(self):
        return self.table

    def insert_into_table(self):
        return "INSERT INTO inv_rough (file_name, file_path, file_size) VALUES (%s, %s, %s)"

    def update_with_hash(self):
        return "UPDATE inv_rough SET file_hash=%s, hashed=1 WHERE id=%s"

    def update_with_examined(self):
        return "UPDATE inv_rough SET hashed=2 WHERE id=%s"

    def add_full_text(self):
        return "ALTER TABLE inv_rough ADD FULLTEXT INDEX 'FullText' (`file_path` ASC, `file_hash` ASC)"

    def get_chunk_for_process(self):
        return "SELECT id, file_path from inv_rough where hashed=0 LIMIT 10000"

class MysqlInvRoughSelects:
    def __init__(self):
        super().__init__()
