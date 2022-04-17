import psycopg2
import pandas as pd
import datetime

db_user = 'mobilserv'
db_password = 'Yqxhtd@51'
db_host = '65.1.154.91'
db_name = 'mobilserv'
db_port = '5432'
conn = None
cur = None

class connection:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            password=db_password,
            port=db_port)
    def fetching(self,customer,machine,startdate,enddate):
        try:
            cur = self.conn.cursor()
            sql='select raw_data->0 as V1 , ts FROM mobilserv.gmiiot_machine_data where '
            if customer is not None:
                sql+=f'customer_id = {customer} '
            if machine is not None:
                sql+=f'and machine_id = {machine} '
            if startdate and enddate is not None:
                if startdate is not int :
                    startdate = int(startdate)
                if enddate is not int:
                    enddate = int(enddate)
                # using local time
                startdate = (datetime.datetime.fromtimestamp(startdate)).strftime("%Y-%m-%d %H:%M:%S")
                enddate = (datetime.datetime.fromtimestamp(enddate)).strftime("%Y-%m-%d %H:%M:%S")
                sql+=f"and ts between '{startdate}' and '{enddate}' order by ts"
            data = pd.read_sql(sql, self.conn)
            return data
        except psycopg2.Error as e:
            print(e)
#
# object = connection()
# object.fetching(machine=34,customer=36,enddate=1648218600,startdate=1648213200)
#

