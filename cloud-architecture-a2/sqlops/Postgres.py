import psycopg2
from psycopg2.extensions import adapt, register_adapter, AsIs
import pandas as pd
import json
import ast
from .pub import sendtokafka
import numpy as np
import re


class Database:
	def __init__(self,cameraId=None,logId=None,human_readable_regex=None):

		"""
		param: human_readbale_regex
			 : A mapping between the regex and the human readable formats
		"""

		self.insert = """INSERT INTO metadata (match,totaltime,trackid,starting,ending,sourceid,speed,length) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
		self.connection = psycopg2.connect(
			host='sensable-2.cesrxorctwi1.us-east-2.rds.amazonaws.com',
			port=5432,
			user='postgres',
			password='sensable01',
			database='demo'
			)

		self.jsonreturn = json.load(open('basereturn.json', "r"))
		self.chart = json.load(open('chart.json',"r"))
		self.human_readable_regex = human_readable_regex
		self._initialize(cameraId,logId)

	def _initialize(self,cameraId,logId):
		"""
		any clean up that needs to happen
		"""

		dict = {}
		if self.human_readable_regex is not None:
			for key,value in self.human_readable_regex.items():
				#new_key = key.split(':')[1]
				dict[key] = value

			for key,value in dict.items():
				self.human_readable_regex[key] = value

		if cameraId is not None:
			self.jsonreturn['cameraId'] = cameraId
			self.jsonreturn['videourl']['key'] =  'out_1920.mp4'
			self.jsonreturn['cameraReportLogId'] = logId



	def send_message(self,ret,source_id):
		# this should load into the database
		cursor = self.connection.cursor()
		# match, totaltime, trackid, starting, ending, sourceid, speed, centroidblob, length
		postgres_insert_query = self.insert
		record_to_insert = (ret['match'],
							ret['totaltime'].total_seconds(),
							ret['trackid'],
							ret['starting'],
							ret['ending'],
							source_id,
							ret['speed'],
							ret['length'],
							)
		cursor.execute(postgres_insert_query, record_to_insert)
		self.connection.commit()
		print('SENDING MESSAGE to DB')


	def create_bar(self,table,report,sql,uniqueid):
		chart = self.chart
		chart['dataSet'][0]['label'] = report
		chart['chartType'] = 'bar'
		chart['title'] = report
		chart['uniqueChartId'] = uniqueid

		chart['labels'] = list(table[list(table)[0]]) #this is going to be the labels
		chart['dataSet'][0]['data']  = list(table[list(table)[1]]) #this is going to be the data

		sql_split = sql.split(' ')[1].split(',')
		chart['x-axis']  = re.sub(r'\([^)]*\)', '', sql_split[0])
		chart['y-axis'] =  re.sub(r'\([^)]*\)', '', sql_split[1])
		self.jsonreturn['chartList'].append(ast.literal_eval(str(chart)))

		return chart

	def clean_data_table(self,df):
		"""
		This will take a custom data table of a metadata table, then esend back
		the human readable format
		"""
		#this will be changed based on the table names provided by the metadata table
		#df = df.rename(columns={'trackid': 'objectid'})

		#this is essentially creating the human readable regex for what the user puts
	

		if len(df['match']) == 0:
			return

		for item in range(len(df['match'])):
			current = df['match'][item]
			df['match'][item] = self.human_readable_regex[current]

		#send back original dataframe by columns
		#return df

		#now this code block will include how to fix the json structure by rows
		df['starting'] = df['starting']   #.dt.strftime('%Y-%m-%d %H:%M:%S')
		df['ending'] = df['ending']       #.dt.strftime('%Y-%m-%d %H:%M:%S')
		table = df.to_dict(orient='records')
		table = ast.literal_eval(json.dumps(table))

		#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		# oonly send the last 150 latest rows of the activity table
		#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		
		print(type(table))
		return table

	def generate_report(self,commands,tablename):

		counter = 0
		for report in commands:
			table = pd.read_sql(commands[report], self.connection)
			#table.to_csv(f'reports/{report}.csv')
			if report.split(':')[0] == 'B':
				v = self.create_bar(table,report.split(':')[2],commands[report],report.split(':')[1])

			if report.split(':')[0] == 'L':
				continue
				#v = self.create_line(table,report.split(':')[1])
				#self.create_line(table,report.split(':')[1])

		# also save the activity table
		table = pd.read_sql(f'SELECT * from {tablename}', self.connection)
		self.jsonreturn['activity_table'] = self.clean_data_table(table)#.to_dict('dict')

		#with open('./indexdemo/PEOPLE-new.json', 'w') as fp:
		#json.dump(self.jsonreturn, fp)
		#only send data if data exists inside activity table
		
		if self.jsonreturn['activity_table'] != None:
			print('**** SENDING REPORT TO UI ***', tablename)
			print(self.jsonreturn)
			sendtokafka(self.jsonreturn)
		






