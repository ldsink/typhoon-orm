import copy

from tornado import gen
from tornado_mysql import pools

from typhoon_orm.database import TDatabase


class TObject(object):
    # *** Configuration Begin ***

    _table_name = None
    _table_columns = {
        'id': 'id',  # id must mapping to database id.
    }
    _db_config = None

    # *** Configuration End ***

    _db_pool = None

    def __init__(self, _id=None):
        self.id = _id
        if not self._table_name:
            raise Exception('No table name defined for {}!'.format(self.__class__.__name__))

    def get_pool(self):
        if not self._db_pool:
            if not isinstance(self._db_config, TDatabase):
                raise Exception('No database configurations for {}!'.format(self.__class__.__name__))
            config = dict(
                host=self._db_config.db_host,
                port=self._db_config.db_port,
                user=self._db_config.db_username,
                passwd=self._db_config.db_password,
                db=self._db_config.db_database,
                charset=self._db_config.db_charset,
                autocommit=self._db_config.db_autocommit)
            self._db_pool = pools.Pool(config, max_idle_connections=1, max_recycle_sec=600, max_open_connections=100)
        return self._db_pool

    @gen.coroutine
    def load(self):
        """
        Load data from database.
        :return:
        """
        columns_name = []
        columns_db_name = []
        for name, db_name in self._table_columns.items():
            if name == 'id':
                continue
            columns_name.append(name)
            columns_db_name.append(db_name)
        query = "SELECT {} FROM {} WHERE id = %(id)s LIMIT 1".format(', '.join(columns_db_name), self._table_name)
        params = {'id': self.id}
        cursor = yield self.get_pool().execute(query, params)
        data = cursor.fetchone()
        if data:
            for i in range(len(columns_name)):
                self.__setattr__(columns_name[i], data[i])
            raise gen.Return(True)
        raise gen.Return(False)

    @gen.coroutine
    def insert(self):
        """
        Insert an object to database.
        :return: last row id
        """
        query = "INSERT INTO {} SET ".format(self._table_name)
        attrs = []
        params = {}
        for item, item_mapping_name in self._table_columns.items():
            if hasattr(self, item) and self.__getattribute__(item) is not None:
                attrs.append("{} = %({})s".format(item_mapping_name, item_mapping_name))
                params[item_mapping_name] = self.__getattribute__(item)
        if not len(attrs):
            raise gen.Return(None)
        query += ', '.join(attrs)
        cursor = yield self.get_pool().execute(query, params)
        raise gen.Return(cursor.lastrowid)

    @gen.coroutine
    def delete(self):
        """
        Delete from database.
        :return:
        """
        query = "DELETE FROM {} WHERE id = %(id)s LIMIT 1".format(self._table_name)
        params = {'id': self.id}
        yield self.get_pool().execute(query, params)

    @gen.coroutine
    def save(self):
        """
        Update value.
        """
        yield self.update()

    @gen.coroutine
    def update(self):
        """
        Update value.
        """
        params = {'id': self.id}
        update_columns = []
        for item, column_name in self._table_columns.items():
            if item in self.__dict__:
                update_columns.append("{} = %({})s".format(column_name, item))
                params[item] = self.__dict__[item]
        if not len(update_columns):
            raise gen.Return(False)
        query = "UPDATE {} SET {} WHERE id = %(id)s LIMIT 1".format(self._table_name, ", ".join(update_columns))
        yield self.get_pool().execute(query, params)
