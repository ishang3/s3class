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

class Delete:
	"""
	This will parse the json and make the necessary upates
	to the database accordingly

	Need to return the bottom:
	1) all updates needed
	2) all deletions needed
	3) camera id; company id
	"""
	def __init__(self, payload):
		#self.data = json.loads(payload)
		self.data = self.test(payload)
		DATABASE_URI = 'postgresql://postgres:sensable01@sensable-2.cesrxorctwi1.us-east-2.rds.amazonaws.com:5432/demo'
		self.engine = create_engine(DATABASE_URI)
		self.meta = MetaData(self.engine)
		#everything below will be parsed by the incoming json
		self.tablename = None
		#make the request to now delete from actual database
		self._makechange()


	def _makechange(self,):
		"""
		after parsing the self.updates, self.delets to make changes to db
		also uses table name parsed from json
		"""
		table = Table(self.tablename, self.meta, autoload=True)
		print(table)
		#this is to be a clear all the data from the table
		# del_stmt = table.delete()
		# self.engine.execute(del_stmt)
		
			
# x = Payload('edit-table.json')


