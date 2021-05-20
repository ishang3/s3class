import json
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, inspect, TIMESTAMP,Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Sequence
import logging

import psycopg2
from psycopg2.extensions import adapt, register_adapter, AsIs
import pandas as pd
import json
import ast
import numpy as np
import re
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import bindparam
import json

class Drop:

    def __init__(self):
        DATABASE_URI = 'postgresql://postgres:sensable01@sensable-2.cesrxorctwi1.us-east-2.rds.amazonaws.com:5432/demo'
        self.engine = create_engine(DATABASE_URI)
        self.meta = MetaData(self.engine)
        self.connection = psycopg2.connect(
            host='sensable-2.cesrxorctwi1.us-east-2.rds.amazonaws.com',
            port=5432,
            user='postgres',
            password='sensable01',
            database='demo'
        )
        self.cursor = self.connection.cursor()


    def generate_tables(self,id,tablename):
        companyname = tablename.split('/')[0]
        company_id = id

        #get all the tables from the database
        ins = inspect(self.engine).get_table_names()

        try:
            for table in ins:
                tablesplit = table.split('_')
                #checking if table matches the params of the incoming json
                if tablesplit[0] == companyname and tablesplit[1] == company_id:
                    self.drop_table(table)
                    print(table)
        except:
            pass

        #finally commit the data
        self.connection.commit()
            

    def drop_table(self,tablename):
        self.cursor.execute(f"DROP TABLE IF EXISTS {tablename}")
        

        


