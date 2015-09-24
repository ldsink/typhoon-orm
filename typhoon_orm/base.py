from tornado import ioloop, gen
from tornado_mysql import pools
import tornado_mysql

from typhoon_orm.database import TDatabase


class TObject(object):
    __table_name = None
    __table_columns = {
        'id': 'id',
    }

    __db_config = None
    __db_pool = None

    def __init__(self, _id=None):
        self._id = _id
        self._data = dict()
        self._need_load = True
        if not self.__table_name:
            raise Exception('No table name defined for {}!'.format(self.__class__.__name__))

    def get_pool(self):
        if not self.__db_pool:
            if not issubclass(self.__db_config, TDatabase):
                raise Exception('No database configurations for {}!'.format(self.__class__.__name__))
            config = dict(
                host=self.__db_config.db_host,
                port=self.__db_config.db_port,
                user=self.__db_config.db_username,
                passwd=self.__db_config.db_password,
                db=self.__db_config.db_database,
                charset=self.__db_config.db_charset,
                autocommit=self.__db_config.db_autocommit)
            self.__db_pool = pools.Pool(config, max_idle_connections=1, max_recycle_sec=600)
        return self.__db_pool

    def reset(self, _id=None):
        if _id is not None:
            self._id = _id
        if not self._id:
            self._id = self._data.get('id', None)
        self._data.clear()
        self._need_load = True

    @gen.coroutine
    def load(self, _id=None):
        self.reset(_id)

        pool = self.get_pool()
        query = "SELECT * FROM %(__table_name)s WHEN id = %(id)s LIMIT = 1"
        params = {
            '__table_name': self.__table_name,
            'id': self._id,
        }
        cursor = yield pool.execute(query, params)
        data = cursor.fetchone()
        if data:
            for k, v in self.__table_columns.items():
                self._data[k] = data.get(v)
            self._need_load = False
        raise gen.Return(self._data)

    @gen.coroutine
    def loads(self, _ids=None):
        if not isinstance(_ids, list):
            raise Exception('In function loads, input param should be a list.')

        pool = self.get_pool()
        query = "SELECT id FROM %(__table_name)s WHEN id IN ({sub_query})"
        params = {'__table_name': self.__table_name}
        sub_query = []
        for pos, _id in enumerate(_ids):
            param = "id_{pos}".format(pos=pos)
            sub_query.append("%({param})s".format(param=param))
            params[param] = _id
        query.format(sub_query=",".join(sub_query))
        cursor = yield pool.execute(query, params)
        data = cursor.fetchall()
        ret = []
        for row in data:
            d = self.__class__(row)
            ret.append(d)
        raise gen.Return(ret)

    @gen.coroutine
    def insert(self):
        query = "INSERT INTO %(__table_name)s SET "
        attrs = []
        params = {
            '__table_name': self.__table_name,
        }
        for item, value in self.__table_columns.items():
            if item in self._data:
                item_value_str = item + 'value'
                attrs.append("{item} = %({value})s".format(item=item, value=item_value_str))
                params[item_value_str] = self._data[item]
        if not len(attrs):
            raise gen.Return(False)
        query += ','.join(attrs)
        cursor = yield self.get_pool().execute(query, params)
        raise gen.Return(cursor.fetchone())

    @gen.coroutine
    def delete(self, _id=None):
        query = "DELETE FROM %(__table_name)s WHERE id = %(id)s LIMIT 1"
        params = {
            '__table_name': self.__table_name,
            'id': _id if _id else self._id,
        }
        cursor = yield self.get_pool().execute(query, params)
        raise gen.Return(cursor.fetchone())

    @gen.coroutine
    def update(self):
        old_data = self._data
        self.load()
        update_attrs = []
        for item, value in self.__table_columns.items():
            if old_data[item] != self._data[item]:
                update_attrs.append((item, old_data[item]))
        if not len(update_attrs):
            raise gen.Return(False)

        query = "UPDATE %(__table_name)s SET {update_attrs} WHERE id = %(id)s LIMIT 1"
        params = {
            '__table_name': self.__table_name,
            'id': self._id,
        }
        attrs = []
        for item, value in update_attrs:
            item_value_str = item + 'value'
            attrs.append("{item} = %({value})s".format(item=item, value=item_value_str))
            params[item_value_str] = self._data[item]
        query = query.format(update_attrs=','.join(attrs))
        cursor = yield self.get_pool().execute(query, params)
        raise gen.Return(cursor.fetchone())

    def __getitem__(self, item):
        if item not in self.__table_columns.keys():
            raise Exception('{object} don\'t have {} field'.format(object=self.__class__.__name__))
        if item not in self._data and self._need_load:
            raise Exception('{object} need load data before get value.'.format(object=self.__class__.__name__))
        return self._data[item]

    def __setitem__(self, item, value):
        if item not in self.__table_columns.keys():
            raise Exception('{object} don\'t have {} field'.format(object=self.__class__.__name__))
        self._data[item] = value


# below for test
class Model(TObject):
    __table_name = 'haha'


a = tornado_mysql.connect()


@gen.coroutine
def main():
    a = Model()
    a.get_pool()


if __name__ == '__main__':
    ioloop.IOLoop.current().run_sync(main)
