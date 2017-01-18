import databases.SqlliteGen as Sql
from classes import SqlLiteFileObj
import os
from pathlib import PurePath as PTH
from pathlib import Path as FullPath
import argparse
import hashlib


def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory_path", help="The path you would like to inventory. All files in all directories "
                                                 "underneath the root path will be scanned.  WARNING: This can take "
                                                 "a very long time.", required=True)
    parser.add_argument("--db_location", help="Path to place the inventory database. The database name will be taken "
                                              "from the final position of your inventory path (e.g. "
                                              "C:\\Staging\\GeoMapp will create a database file called 'GeoMapp.db' "
                                              "in the location indicated", required=True)
    return parser.parse_args()


def process_files(f):
    for f in files:
        print(f)
        hashlib.sha256.update(str(f).encode())


def process_folders(fldr, f):
    print()


if __name__ == "__main__":
    args = arg_parse()
    inv_path = PTH(args.inventory_path)
    #db_location = PTH(args.db_location, inv_path._parts[-1] +".db")

    #mt = Sql.config.CreateMainTable(db_location)

    #if not mt.main_table_created():
    #    Sql.create(mt.create(), mt.sqlite_file)
    sha256_now = hashlib.sha256()
    lvl1_hash = []
    lvl2_hash = []
    lvl3_hash = []
    lvl4_hash = []

    for dirpath, dirs, files in os.walk(str(inv_path), topdown=False):
        depth = len(dirpath.split(os.path.sep))
        if depth == 1:
            pass
        if depth == 2:
            pass
        if depth == 3:
            pass
        if depth == 4:
            lvl4_hash.append(sha256_now.hexdigest())
            sha256_now = hashlib.sha256()

        for f in files:
            if f == "Thumbs.db":
                continue
            fp = os.path.join(dirpath, f)
            print(fp)
            fni = {fp: list(os.lstat(fp))}
            sha256_now.update(str(fni).encode("utf-8"))
        # Okay I have files for a directory
        # How deep in the tree am I should I roll up this hash to another tree?
        #print("Dirpath={} :: Hash={}".format(dirpath, sha256_now.hexdigest()))

