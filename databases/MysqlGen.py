import mysql.connector
from mysql.connector import errorcode


class MySqlConnector:

    def __init__(self, config):
        self.config = config
        self.db_ok = False
        try:
            _cnx = mysql.connector.connect(**self.config)
            self.db_ok = True
            _cnx.close()
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

    def get_connector(self):
        return mysql.connector.connect(**self.config)