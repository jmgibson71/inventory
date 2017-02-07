import classes.SampleHash as sh
import databases.PyMysqlGen as msql
import os
import argparse
import logging
import databases.TableConfig as cfg
import configparser
from classes.LocalLogger import HoldingsLogger
from classes.LocalLogger import ReportHandler
from time import gmtime, strftime
from hurry.filesize import size


class Configurations:
    def __init__(self):
        super().__init__()
        self.config = None

    def arg_parse(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--compare-path", help="The path that you would like to compare against an inventory db.")
        parser.add_argument("--compare-db", help="The name of the database you will be comparing against. If you do not "
                                                 "know the options use the -l ")
        parser.add_argument("-l", action='store_true', help="List the available databases.")

        parser.add_argument("-s", action='store_true', help="If you are comparing the same source " \
                                                                              "as the indexed database add this flag")
        return parser.parse_args()

    def list_databases(self):
        configure = configparser.ConfigParser()
        configure.read(os.path.join(os.getcwd(), "db_config.cfg"))
        self.config = {'user': configure['DATABASE']['USER'],
                  'password': configure['DATABASE']['PASS'],
                  'host': configure['DATABASE']['HOST'],
                  'database': 'mysql'
                  }
        conn = msql.PyMySqlConnector(self.config).get_connector()
        cur = conn.cursor()
        query = "SELECT TABLE_SCHEMA from information_schema.tables where TABLE_NAME = 'inv_rough'"
        cur.execute(query)
        count = 1
        table = {}
        for line in cur.fetchall():
            table[count] = line[0]
        return table

    def show_db_options(self, dbs):
        print("Here are the available Databases to match against:")
        for o, name in dbs.items():
            print("{}\t{}".format(o, name))
        num = input("Enter the number of the Database you want to use: ")
        return dbs.get(int(num))

    def build_logger(self):
        name = "holdings.log"
        HoldingsLogger(name, 'a')


class CompareSourceToDB:
    def __init__(self, config):
        """
        :type source : str
        :param database identifier:
        """
        self.logger = logging.getLogger("CompareSource")
        self.source = msql.PyMySqlConnector(config)

        self.compare_same_source = False
        self.unique_files = []
        self.report_line = []
        self.match_buffer = {}
        self.current_fn = None
        self.matches_log = None
        self.unique_log = None
        self.match_size = 0
        self.mark_query = cfg.MysqlCreateMainTable().update_with_examined()
        self.match_rep = None
        self.unique_rep = None

    def set_matches_name(self, nme):
        self.matches_log = "{}_{}.log".format(nme, strftime("%Y-%m-%d_%H%M%S", gmtime()))
        self.match_rep = ReportHandler(self.matches_log)

    def set_unique_name(self, nme):
        self.unique_log = "{}_{}.log".format(nme, strftime("%Y-%m-%d_%H%M%S", gmtime()))
        self.unique_rep = ReportHandler(self.unique_log)

    def compare_from_db(self):
        conn = self.source.get_connector()
        cur = conn.cursor()
        x = 0
        while True:
            query = "SELECT * FROM inv_rough WHERE hashed = 1 LIMIT 1"
            cur.execute(query)
            if cur.rowcount == 0:
                break
            row = cur.fetchone()
            print("Checking: {} ({})".format(row[0], str(row[2]).encode("utf-8")))
            self.mark_as_checked(row[0])
            if row[1] == 'Thumbs.db' or row[1] == 'bagit.txt' or row[3] == 0:
                self.mark_as_checked(row[0])
                continue
            self.current_fn = row[2]
            self.compare_hash(row[4])   # row[4] is the hash value
        self.write_stats()

    def write_stats(self):
        name = "{}_{}.log".format("stats", strftime("%Y-%m-%d_%H%M%S", gmtime()))
        rh = ReportHandler(name)
        fh = rh.get_file_handle("w")
        fh.write("{} of duplicate data found.\n".format(size(self.match_size)))
        fh.close()

    def mark_as_checked(self, id):
        conn = self.source.get_connector()
        cur = conn.cursor()
        cur.execute(self.mark_query, id)
        cur.close()

    def compare_hash(self, hsh):
        conn = self.source.get_connector()
        cur = conn.cursor()
        # Make sure that you only select items that have not matched and been reported before.
        query = "SELECT * from inv_rough where file_hash = '{}' and hashed = 1".format(hsh)
        cur.execute(query)
        if self.is_unique(cur):
            # Already checked
            pass
        else:
            self.add_report_lines(cur)
            if len(self.match_buffer) == 10:
                self.write_report_lines()
                self.match_buffer = {}

    def file_to_compare(self, f):
        """
        :type f : str
        :param f:
        :return:
        """

        self.current_fn = f
        print("Checking File: {}".format(f))
        my_hash = sh.SampleHash(f)
        comp = my_hash.hash()
        conn = self.source.get_connector()
        cur = conn.cursor()
        query = "SELECT * from inv_rough where file_hash = '{}' and hashed = 1".format(comp)
        cur.execute(query)
        try:
            if self.is_unique(cur):
                # handle
                self.unique_files.append(f)
                if len(self.unique_files) == 2000:
                    self.write_unique_lines()
            else:
                # Log Matches
                self.add_report_lines(cur)
                if len(self.match_buffer) == 100:
                    self.write_report_lines()
                    self.match_buffer = {}
        except CompareError as e:
            self.logger.error("{}: {}".format(f, e))

    @staticmethod
    def get_common_path(comp, mtch):
        """
        get_common_path is a reporting helper function that compares two file paths to find the parts of the path
        that are identical to the matched file. The function returns a tuple of lists 0: is the common path,
        1: is the path in list form from the point of divergence, 2: is the same
        :param comp: str
        :param mtch: str
        :return: tuple
        """
        common_path = ''
        compare = comp.split(os.sep)
        match = mtch[2].split(os.sep)
        for p, x in enumerate(comp.split(os.sep)):
            if not p < (len(compare) - 1):
                common_path = match[0:p-1]
                break
            if x not in match or not p < (len(compare) - 1):
                common_path = match[0:p]
                break
        return common_path, compare[p:], match[p:]

    def write_report_lines(self):
        """
        Takes the matches in the match buffer and writes to the log.  The match buffer contains a dictionary the key of
        which is the matched file, and contains another dictionary, of compares.
        :return:
        """
        compare_rep = self.match_rep.get_file_handle("a")
        for file, matches in self.match_buffer.items():
            compare_rep.write(file + "\n")
            for common, matches in matches.items():
                compare_rep.write("\tCP:\t{}\n".format(common))
                for m in matches:
                    compare_rep.write("\t\t{}\n".format(m))
            compare_rep.write("\n\n")
        compare_rep.close()

    def write_unique_lines(self):
        fn = open(self.unique_log, "a")
        for line in self.unique_files:
            fn.write(line + "\n")
        fn.close()
        self.unique_files = []

    def add_report_lines(self, cur):
        """
        Takes an sql result set and sorts into a dictionary of common paths. The key of the dictionary is a path fragment
        that is the common path of the matches to the matched file.
        :type cur: pymysql.cursor
        :param cur:
        :return dict:
        """
        common_paths = {}
        for row in cur.fetchall():
            self.mark_as_checked(row[0])
            tup = self.get_common_path(self.current_fn, row)
            if tup[0] == '':  # There is no common path this shouldn't happen on a same source comparison
                common_paths['No Common Path'].append(tup[2])
                continue
            if not os.path.join(*tup[0]) in common_paths:  # This
                common_paths[os.path.join(*tup[0])] = []
            common_paths[os.path.join(*tup[0])].append(tup[2])
        self.match_buffer[self.current_fn] = common_paths

    @staticmethod
    def is_unique(cur):
        """
        Helper function. Potetntially refactor
        :type cur: pymysql.cursor
        :param cur:
        :return Boolean:
        """
        if cur.rowcount == 0:
            # No Match
            return True
        if cur.rowcount > 0:
            # Match
            return False


class CompareError(Exception):
    pass

if __name__ == "__main__":
    configure = Configurations()
    configure.build_logger()
    logger = logging.getLogger("main")
    args = configure.arg_parse()

    if args.l:
        dbs = configure.list_databases()
        args.compare_db = configure.show_db_options(dbs)
        configure.config['database'] = args.compare_db

    compare = CompareSourceToDB(configure.config)
    if args.s:
        compare.set_matches_name("{}_match".format(args.compare_db))
        compare.set_unique_name("{}_unique".format(args.compare_db))
        compare.compare_same_source = True
        compare.compare_from_db()
        compare.write_report_lines()
    else:
        compare.matches_log = str(args.compare_path).replace(os.sep, "_") + "_match.txt"
        compare.unique_log = str(args.compare_path).replace(os.sep, "_") + "_unique.txt"
        for dirpath, _, files in os.walk(args.compare_path):
            for f in files:
                if f == "Thumbs.db":
                    continue
                if f == "bagit.txt":
                    continue
                compare.file_to_compare(os.path.join(dirpath, f))
        compare.write_unique_lines()
        compare.write_report_lines()