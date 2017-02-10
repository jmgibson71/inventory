import databases.PyMysqlGen as pymy
import databases.TableConfig as tables
import pymysql.cursors


class InventoryChunk:
    def __init__(self, db_config):
        """
        :type self.pycon :
        :param db_config:
        """
        self.config = db_config
        self.pycon = pymy.PyMySqlConnector(db_config)  # type : database.PyMysqlGen
        self.pycur = None   # type : pymysql.cursors
        self.results = True
        self.count = 0

    def get_process_package(self):
        pycon = self.pycon.get_connector()
        self.pycur = pycon.cursor()
        self.pycur.execute(tables.MysqlCreateMainTable().get_chunk_for_process())
        if self.pycur.rowcount > 0:
            self.results = True
            self.count += self.pycur.rowcount
        else:
            self.results = False

    def get_results(self):
        l = self.pycur.fetchall()
        self.pycur.close()
        return l