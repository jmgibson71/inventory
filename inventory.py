import databases.MysqlGen as msql
import databases.PyMysqlGen as pymy
from classes import MysqlFileObj
import os
from pathlib import PurePath as PTH
import logging
import argparse
import databases.TableConfig as cfg
import classes.SampleHash as shash
import mysql.connector.errors
from classes.ProcessPackage import InventoryChunk as IC


def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory-path", help="The path you would like to inventory. All files in all directories "
                                                 "underneath the root path will be scanned.  WARNING: This can take "
                                                 "a very long time.", required=True)
    parser.add_argument("--db-location", help="Path to place the inventory database. The database name will be taken "
                                              "from the final position of your inventory path (e.g. "
                                              "C:\\Staging\\GeoMapp will create a database file called 'GeoMapp.db' "
                                              "in the location indicated")
    parser.add_argument("--excluded-paths", help="A path to a text file that has file paths you wish to exclude."
                                                 " One path per line.")

    parser.add_argument("--database-name", help="A name to hold the inventory", default='inventory')

    parser.add_argument("--pass-number", help="The number of the pass on the inventory.  1=First Pass: builds file list",
                        default=0)

    return parser.parse_args()


def first_walk(inv_path, db_config):
    mt = msql.MySqlConnector(db_config)
    con = mt.get_connector()

    for dirpath, dirs, files in os.walk(inv_path, topdown=True):
        for f in files:
            full_path = os.path.normpath(os.path.join(dirpath, f))
            if not os.path.exists(full_path):
                logger.error("Problem with the file path: {} \nFile not added to database".format(full_path.encode("utf-8")))
                continue
            fob = MysqlFileObj.FileMap(full_path)
            query = cfg.MysqlCreateMainTable().insert_into_table()
            cur = con.cursor()
            cur.execute(query, (fob.file_name, fob.file_path, fob.file_size))
            print("{}:{}".format(cur.lastrowid, fob.file_path.encode("utf-8")))
            con.commit()
    con.close()


def second_walk(db_config):
    mt = msql.MySqlConnector(db_config)
    con = mt.get_connector()
    ic = IC(db_config)
    while ic.results:
        print("Processing inventory chunk {} through {}".format(ic.count, ic.count+10000))
        ic.get_process_package()
        for id, path in ic.get_results():
            if not os.path.exists(path):
                logger.error("Problem with the file path: {} \nFile not hashed".format(path.encode("utf-8")))
                continue
            fob = MysqlFileObj.FileMap(path)
            hsh = shash.SampleHash(fob.file_path).hash()
            query = cfg.MysqlCreateMainTable().update_with_hash()
            cur = con.cursor()
            cur.execute(query, (hsh, id))
            print("{}:{}".format(id, path.encode("utf-8")))
            con.commit()
    con.close()


def get_excluded_dirs(fp):
    excludes = []
    with open(fp, 'r') as f:
        for l in f.readlines():
            excludes.append(l.strip())
    return excludes


def setup_db(db_nm):
    config = {
        'user': 'jeremy',
        'password': 'C12mp3b1',
        'host': '127.0.0.1',
        'database': db_nm
    }

    mysqlconn = msql.MySqlConnector(config)
    if mysqlconn.db_ok:
        return config
    else:
        try:
            db_name = config.pop("database")
            msg = msql.MySqlConnector(config)
            conn = mysqlconn.get_connector()
            cur = conn.cursor()
            cur.execute("""CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'""".format(db_name))
            conn.commit()
            cur.close()
            conn.close()
            config['database'] = db_name
            return config
        except mysql.connector.errors.Error as e:
            print(e)
            return None


def table_setup(config):
    pmycon = pymy.PyMySqlConnector(config['database'])
    con = pmycon.get_connector()
    cur = con.cursor()
    cur.execute("SHOW TABLES LIKE 'inv_rough'")
    if cur.rowcount == 1: return
    cur.execute(cfg.MysqlCreateMainTable().create())
    con.close()


if __name__ == "__main__":
    logging.basicConfig(filename='holdings.log', level=logging.DEBUG)
    logger = logging.getLogger()
    args = arg_parse()
    inv_path = args.inventory_path
    if args.db_location is not None:
        db_location = PTH(args.db_location, inv_path._parts[-1] + ".db")
    db_name = args.database_name
    excludes = []

    if args.excluded_paths:
        excludes = get_excluded_dirs(args.excluded_paths)

    config = setup_db(db_name)
    table_setup(config)

    if args.pass_number == str(0):
        first_walk(inv_path, config)
    if args.pass_number == str(1):
        second_walk(config)
