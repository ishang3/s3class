from sqlalchemy import create_engine
#from models import Base
import datetime
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, inspect, TIMESTAMP,Float, Time
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Sequence


class MessageSending:
	def __init__(self,activity_table_name,presence_table):
		DATABASE_URI = 'postgresql://postgres:sensable01@sensable-2.cesrxorctwi1.us-east-2.rds.amazonaws.com:5432/demo'
		self.engine = create_engine(DATABASE_URI)
		self.activitytable,self.presencetable = self.TableCreator(activity_table_name,presence_table)
		self.Session = sessionmaker(bind=self.engine)


	#this is sending the value to the presence table
	def send_objectid(self,payload):
		message = self.presencetable(
			day  = payload['day'],
			trackid = payload['trackid'],
			type =    payload['type'],
			timeshort =  payload['timeshort'],
			starting =  payload['starting'],
			ending   =   payload['ending'],
			objectvalue = payload['objectvalue'] 
		)
		s = self.Session()
		s.add(message)
		s.commit()

	#this 
	#this will be sending a match results to the activity table of the camera source
	def send_match(self,payload):
		message = self.activitytable(
			trackid=payload['trackid'],
			match=payload['match'],
			starting=payload['startime'],
			ending=payload['endtime'],
			totaltime=payload['totaltime'],
			#multiple json fields for below
			relativestarting=payload['relativestarting'],
			relativending =payload['relativending'],
			endingsource=payload['endingsource'],
			startingsource=payload['startingsource']
		)
		s = self.Session()
		s.add(message)
		s.commit()
		#s.close()


	#need to move to a helper function
	def TableCreator(self,activitytable,presencetable):
		class ActivityTable(declarative_base()):
			__tablename__ = activitytable
			uniquerowid = Column(Integer,
								 Sequence(f'{activitytable}_id_seq', start=1, increment=1),
								 primary_key=True)
			trackid = Column(String)
			match = Column(String)
			#object_type = Column(String) #new addition
			starting = Column(TIMESTAMP(timezone=False))
			totaltime= Column(Float)
			ending = Column(TIMESTAMP(timezone=False))
			#multiple json feature
			relativestarting = Column(TIMESTAMP(timezone=False))
			relativending    = Column(TIMESTAMP(timezone=False))
			endingsource = Column(String)
			startingsource = Column(String)


		class PresenceTable(declarative_base()):
			__tablename__ = presencetable
			uniquerowid = Column(Integer,
								 default=1,
								 primary_key=True,
								 )
			day = Column(Integer)
			trackid = Column(Integer)
			type = Column(String)
			timeshort = Column(Time)
			starting = Column(TIMESTAMP(timezone=False))
			ending = Column(TIMESTAMP(timezone=False))
			objectvalue = Column(String)


		return ActivityTable,PresenceTable









