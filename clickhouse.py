from clickhouse_driver import Client


class ClickHouse:
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    def connect(self):
        self.client = Client(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database)

    def execute(self, query):
        # print(query)
        return self.client.execute(query)

    def insert(self, query, data):
        return self.client.insert_dataframe(query, data)

    def close(self):
        self.client.disconnect()


if __name__ == '__main__':
    database = ClickHouse('10.26.22.117', 9001, 'default', 'galaxy2019', 'new')
    database.connect()
    database.execute("INSERT INTO FEEDBACK (ID, AGENT_TYPE, FEEDBACK_TYPE, FEEDBACK, FEEDBACK_TIME, IS_COMPLETED) "
                     f"VALUES (generateUUIDv4(), {1}, {0}, '测试abcd-%￥#……*（12234555', now(), 0)")
