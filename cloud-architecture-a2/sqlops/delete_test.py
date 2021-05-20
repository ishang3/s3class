import json
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, inspect, TIMESTAMP,Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Sequence

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

class Clear:

    def __init__(self):
        DATABASE_URI = 'postgresql://postgres:sensable01@sensable-2.cesrxorctwi1.us-east-2.rds.amazonaws.com:5432/demo'
        self.engine = create_engine(DATABASE_URI)
        self.meta = MetaData(self.engine)

    def generate_names(self,id,tablename):
        companyname = tablename.split('/')[0]
        activity_table = companyname + '_' + str(id)
    
        self.delete(activity_table.lower())

        presence_table = companyname + '_' + str(id) + '_' + 'presence'
        self.delete(presence_table.lower())

    
    def delete(self,tablename):
        #incase table doesnt exist in db
        try:
            table = Table(tablename, self.meta, autoload=True)
            del_stmt = table.delete()
            self.engine.execute(del_stmt)

        except:
            print('table does not exist')


# del1 = Delete()