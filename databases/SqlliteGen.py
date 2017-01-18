import apsw as sqlite3
import databases.TableConfig as config


def create(stmt, file):
    conn = sqlite3.Connection(str(file))
    c = conn.cursor()
    c.execute(stmt)
    c.close()
    conn.close()


def get_connection(file):
    return sqlite3.Connection(str(file))

def get_cursor():
    pass