import classes.SampleHash as sh
import databases.PyMysqlGen as msql
import os
import argparse
import logging


def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument("--compare-path", help="The path that you would like to compare against an inventory db."
                                                 , required=True)
    parser.add_argument("--compare-db", help="The name of the database you will be comparing against. If you do not "
                                             "know the options use the -l ")
    parser.add_argument("-l", action='store_true', help="List the available databases.")

    parser.add_argument("-s", action='store_true', help="If you are comparing the same source " \
                                                                          "as the indexed database add this flag")

    return parser.parse_args()


def list_databases():
    conn = msql.PyMySqlConnector().get_connector()
    cur = conn.cursor()
    query = "SELECT TABLE_SCHEMA from information_schema.tables where TABLE_NAME = 'inv_rough'"
    cur.execute(query)
    count = 1
    table = {}
    for line in cur.fetchall():
        table[count] = line[0]
    return table


def show_db_options(dbs):
    print("Here are the available Databases to match against:")
    for o, name in dbs.items():
        print("{}\t{}".format(o, name))
    num = input("Enter the number of the Database you want to use: ")
    return dbs.get(int(num))


def build_logger():
    # set up logging to file - see previous section for more details
    logging.basicConfig(level=logging.DEBUG, filename='compare.log', filemode='w')
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)


class CompareSourceToDB:
    def __init__(self, source=None):
        """
        :type source : str
        :param database identifier:
        """
        self.logger = logging.getLogger("CompareSource")
        if source is None:
            self.source = msql.PyMySqlConnector()
        else:
            self.source = msql.PyMySqlConnector(source)

        self.compare_same_source = False
        self.unique_files = []
        self.report_line = []
        self.match_buffer = {}
        self.current_fn = None
        self.matches_log = None
        self.unique_log = None

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
        query = "SELECT * from inv_rough where file_hash LIKE '{}'".format(comp)
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
                    self.write_report_line()
                    self.match_buffer = {}
        except CompareError as e:
            self.logger.error("{}: {}".format(f, e))

    @staticmethod
    def get_common_path(comp, mtch):
        common_path = ''
        compare = comp.split(os.sep)
        match = mtch[2].split(os.sep)
        for p, x in enumerate(comp.split(os.sep)):
            if x not in match:
                common_path = match[0:p]
                break
        return common_path, compare[p:], match[p:]

    def write_report_line(self):
        compare_rep = open(self.matches_log, "a")
        for file, matches in self.match_buffer.items():
            compare_rep.write(file + "\n")
            for m in matches:
                compare_rep.write("\t{}\n".format(m))
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
        :type cur: pymysql.cursor
        :param cur:
        :return:
        """
        lines = []
        for row in cur.fetchall():
            tup = self.get_common_path(self.current_fn, row)
            lines.append(tup[2])
        self.match_buffer[self.current_fn] = lines

    def is_unique(self, cur):
        """
        :type cur: pymysql.cursor
        :param cur:
        :return Boolean:
        """
        if self.compare_same_source and cur.rowcount == 0:
            # This is some sort of error.  Most likely the file was indexed and then removed before the compare
            # process was run
            raise CompareError("Results are 0 on a compare same source process.  Most likely the db you are "
                               "comparing is out of date.")
        if self.compare_same_source and cur.rowcount == 1:
            # No Match
            return True
        if self.compare_same_source and cur.rowcount > 1:
            # Match
            return False

        if not self.compare_same_source and cur.rowcount == 0:
            # No Match
            return True

        if not self.compare_same_source and cur.rowcount > 0:
            # Match
            return False


class CompareError(Exception):
    pass

if __name__ == "__main__":
    build_logger()
    logger = logging.getLogger("main")
    args = arg_parse()

    if args.l:
        dbs = list_databases()
        args.compare_db = show_db_options(dbs)

    compare = CompareSourceToDB(args.compare_db)
    compare.matches_log = str(args.compare_path).replace(os.sep, "_") + "_match.txt"
    compare.unique_log = str(args.compare_path).replace(os.sep, "_") + "_unique.txt"
    if args.s:
        compare.compare_same_source = True
    for dirpath, _, files in os.walk(args.compare_path):
        for f in files:
            if f == "Thumbs.db":
                continue
            if f == "bagit.txt":
                continue
            compare.file_to_compare(os.path.join(dirpath, f))
    compare.write_unique_lines()
    compare.write_report_line()