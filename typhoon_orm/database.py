class TDatabase(object):
    db_host = '127.0.0.1'
    db_port = 3306
    db_username = None
    db_password = None
    db_database = None
    db_charset = 'utf8'
    db_autocommit = True


class UserDB(TDatabase):
    DB_PORT = 3307


if __name__ == '__main__':
    a = UserDB()
    a.db_port = 10
    print(a.db_port)
