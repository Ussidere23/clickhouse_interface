import clickhouse_connect


class Clickhouse:

    def __init__(self, host, db_name, db_username, db_password):
        self.__host = host
        self.__db_name = db_name
        self.__username = db_username
        self.__password = db_password
        self.query = Query(self.__host, self.__db_name, self.__username, self.__password)


class Query:
    def __init__(self, host, name, username, password):
        self.client = clickhouse_connect.get_client(host=host,
                                                    database=name,
                                                    username=username,
                                                    password=password)

    def insert(self, table_name, data, column_names):
        return self.client.insert(table_name, data, column_names=column_names)

    def get(self, table_name, column_names, where):
        query = 'SELECT '
        if column_names:
            query = query + ', '.join(column_names) + f' FROM {table_name} '
        else:
            query += f'* FROM {table_name} '
        if where:
            query += 'WHERE ' + 'and '.join(where)
        return self.client.query(query)

    def delete(self, table_name, where):
        query = f'DELETE {table_name} '
        if where:
            query += 'WHERE ' + 'and '.join(where)
        return self.client.query(query)
