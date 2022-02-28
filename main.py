import mysql.connector as mysql
from mysql.connector import Error
import sqlalchemy
from urllib.parse import quote_plus as urlquote
import matplotlib.pyplot as plt
import pandas as pd
import os

class assig:
    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    def create_db(self, db_name):
        conn = mysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.password
        )
        try:
            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute("CREATE DATABASE {}".format(db_name))
        except Error as e:
            print("Error while connecting to MySQL", e)
        # preparing a cursor object
        # creating database

    def create_table(self, db_name, table_name, df):
        conn = mysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.password
        )
        try:
            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute("USE {}".format(db_name))
                cursor.execute("CREATE TABLE {}".format(table_name))
        except Error as e:
            print("Error while connecting to MySQL", e)

        engine_stmt = 'mysql+mysqldb://%s:%s@%s:%s/%s' % (self.user, urlquote(self.password),
                                                          self.host, self.port, db_name)
        engine = sqlalchemy.create_engine(engine_stmt)

        df.to_sql(name=table_name, con=engine,
                  if_exists='append', index=False, chunksize=1000)

    def load_data(self, db_name, table_name):
        conn = mysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.password
        )
        try:
            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM {}.{}".format(db_name, table_name))
                result = cursor.fetchall()
                return result
        except Error as e:
            print("Error while connecting to MySQL", e)

    def import_csv(self, path):
        return pd.read_csv(r"C:\Users\USER\Desktop\Temp\scoring.csv", index_col=False , delimiter=',')


if __name__ == '__main__':
    dbb1 = assig("localhost", "3306", "root", "")
    dbb1.create_db("MarketPlace")
    df = dbb1.import_csv("CoinMarket 28 Feb 2022 - Sheet1.csv")
    dbb1.create_table("MarketPlace", "CoinMarket 28 Feb 2022 - Sheet1", df)
    print(dbb1.load_data("MarketPlace", "CoinMarket 28 Feb 2022 - Sheet1"))