import pymysql


class PyMySqlConnector:

    def __init__(self, config):
        self.config = config
        self._cnx = None  # type: pymysql.connections.Connection

    def database_exists(self):
        try:
            conn = pymysql.connections.Connection(**self.config)
            self._cnx = self.get_connector()
            cur = self._cnx.cursor()
            cur.execute("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{}'"
                        .format(self.config['database']))
        except pymysql.err.InternalError as e:
            return False
        except TypeError as e:
            return False
        if cur.rowcount == 1:
            self._cnx.close()
            return True
        else:
            self._cnx.close()
            return False

    def create_database(self, db_name):
        self._cnx = self.get_connector()
        cur = self._cnx.cursor()
        query = "CREATE DATABASE {}".format(db_name)
        cur.execute(query)
        cur.close()
        self._cnx.close()
        self.config['database'] = db_name

    def get_connector(self):
        if self._cnx:
            return self._cnx
        else:
            self._cnx = pymysql.connect(**self.config)
        return self._cnx

    def get_cursor(self):
        return self._cnx.cursor()  # type: pymysql.cursors

    def get_last_insert(self, cur):
        cur.execute("SELECT LAST_INSERT_ID()")
        return self._cnx.cursor()

    def does_table_exist(self, tbln):
        res = self._cnx.cursor().execute("SHOW TABLES LIKE '{}%'".format(tbln))
        print(res)