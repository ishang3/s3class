import psycopg2
from psycopg2.extensions import adapt, register_adapter, AsIs
import pandas as pd
import json
import ast
from .pub import sendtokafka
import numpy as np
import re
import csv


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

		self.jsonreturn = json.load(open('./ondemand/basereturn.json', "r"))
		self.chart = json.load(open('./ondemand/chart.json',"r"))
		self.flexibletableformat = json.load(open('./ondemand/flexibletable.json',"r"))
		self.videosource_hls = json.load(open('./ondemand/videosource_hls.json',"r"))
		self.videosource_s3bucket = json.load(open('./ondemand/videosource_s3bucket.json',"r"))
		self.videosource_signurl =  json.load(open('./ondemand/videosource_signurl.json',"r"))
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
			self.jsonreturn['cameraReportLogId'] = logId


	def create_bar(self,table,report,sql,uniqueid,xaxistitle,yaxistitle,type):
		chart = self.chart
		chart['dataSet'][0]['label'] = report
		chart['chartType'] = type
		chart['title'] = report
		chart['uniqueChartId'] = uniqueid

		chart['labels'] = list(table[list(table)[0]]) #this is going to be the labels
		chart['dataSet'][0]['data']  = list(table[list(table)[1]]) #this is going to be the data

		chart['x-axis']  = xaxistitle 
		chart['y-axis'] =  yaxistitle
		
		self.jsonreturn['chartList'].append(ast.literal_eval(str(chart)))

		return chart

	def create_flexible_table(self,tableid,tablename,df):
		"""
		
		"""
		flex = self.flexibletableformat.copy()
		final = {}

		flex['tableId'] = tableid
		flex['cols'] = list(df.columns)

		#check for any values, and convert to string
		for colname,value in df.dtypes.items():
			if colname != 'uniquerowid':
				df[colname] = df[colname].astype(str)

		#get all the rows and add all rows
		rows = df.to_dict(orient='records')
		flex['rows'] = rows

		#return final dict
		ret = final.copy()
		ret[tablename] = flex

		if tablename not in self.jsonreturn['activity_table']:
			self.jsonreturn['activity_table'][tablename] = flex
			

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
		df['starting'] = df['starting'].dt.strftime('%Y-%m-%d %H:%M:%S')
		df['ending'] = df['ending'].dt.strftime('%Y-%m-%d %H:%M:%S')
		table = df.to_dict(orient='records')

		table = ast.literal_eval(json.dumps(table))
	
	def create_video(self,videotype,videoid,name,videosource):
		"""
		"""
		if videotype == 'hls':
			hlsjson = self.videosource_hls.copy()
			hlsjson['videoId'] = videoid
			hlsjson['videoName'] = name
			hlsjson['hlsUrl']   = videosource

			#add to master json
			self.jsonreturn['videoList'].append(hlsjson)

			
		if videotype == 's3':
			s3json = self.videosource_s3bucket.copy()
			s3json['videoId'] = videoid 
			s3json['videoName'] = name
			bucket = videosource.split(':')[0]
			key = videosource.split(':')[1]
			s3json['videoUrl']['bucket']  = bucket
			s3json['videoUrl']['key'] = key

			#add to master json
			self.jsonreturn['videoList'].append(s3json)

	


	def generate_report(self,commands=None,tablename=None):

		"""
		This is trying to generate the reports for incoming commands 
		"""

		for report in commands:

			#handle video return link grammar -> Video:{TYPE}:{ID}:{NAME}
			if report.split(':')[0].lower() == 'video':
				videotype = report.split(':')[1]
				videoid = report.split(':')[2]
				videoname = report.split(':')[3]

				self.create_video(videotype,videoid,videoname,commands[report])

				continue

			#run command sql query for generating charts, tables,
			table = pd.read_sql(commands[report], self.connection)
			#table.to_csv(f'reports/{report}.csv')
			if report.split(':')[0].lower() == 'db':
				xaxistitle = report.split(':')[3]
				yaxistitle = report.split(':')[4]
				v = self.create_bar(table,report.split(':')[2],commands[report],report.split(':')[1],xaxistitle,yaxistitle,'bar')

			if report.split(':')[0].lower() == 'di':
				xaxistitle = report.split(':')[3]
				yaxistitle = report.split(':')[4]
				v = self.create_bar(table, report.split(':')[2], commands[report], report.split(':')[1], xaxistitle, yaxistitle,'line')

			if report.split(':')[0].lower() == 'table':
				tableid = report.split(':')[1]
				tablename = report.split(':')[2]
				ret = self.create_flexible_table(tableid,tablename,table)
			
			#adding pie chart to 
			if report.split(':')[0].lower() == 'pie':
				tableid = report.split(':')[1]
				piename = report.split(':')[2]
				xaxistitle = report.split(':')[3]
				yaxistitle = report.split(':')[4]
				#table,report,sql,uniqueid,xaxistitle,yaxistitle,type
				v = self.create_bar(table,piename,commands[report],tableid,xaxistitle,yaxistitle,'pie')
				pass

			#adding multi line chart 
			if report.split(':')[0].lower() == 'multiline':
				tableid = report.split(':')[1]
				tablename = report.split(':')[2]
				pass

			

	

			#3 intersection
			#   
		
		
		sendtokafka(self.jsonreturn)
		return

		# also save the activity table
		table = pd.read_sql(f'SELECT * from {tablename}', self.connection)
		self.jsonreturn['activity_table'] = self.clean_data_table(table)#.to_dict('dict')

		# with open('./indexdemo/PEOPLE-new.json', 'w') as fp:
		# 	json.dump(self.jsonreturn, fp)
		#only send data if data exists inside activity table
		if self.jsonreturn['activity_table'] != None:
			print('**** SENDING REPORT TO UI ***', tablename)
			sendtokafka(self.jsonreturn)
		
# test = Database()
# test.generate_report()



#select uniquerowid,trackid,match,ending,totaltime, to_char(starting,'HH:MM:SS.MS') as starting from sensable_269 limit 10

"""
select uniquerowid,trackid,match,ending,totaltime, to_char(starting,'HH24:MI:SS.MS') as starting
from sensable_269
order by starting
limit 10

"""