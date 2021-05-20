from sqlalchemy import create_engine
#from models import Base
import datetime
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, inspect, TIMESTAMP,Float, Time
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Sequence


class TempSending:
	def __init__(self,):
		DATABASE_URI = 'postgresql://postgres:sensable01@sensable-2.cesrxorctwi1.us-east-2.rds.amazonaws.com:5432/demo'
		self.engine = create_engine(DATABASE_URI)
		self.temposetable = self.TableCreator2('nestle22')
		self.Session = sessionmaker(bind=self.engine)


	#this is sending the value to the presence table
	def send_tempobjectid(self,payload):
		message = self.temposetable(
			frameid  = payload['frameid'],
			trackid = payload['trackid'],
			delta =    payload['delta'],
			deltatime =  payload['deltatime'],
			pa  =  payload['pa'],
			pax = payload['pax'],
			pay = payload['pay'],
			pb  =   payload['pb'],
			pbx = payload['pbx'],
			pby = payload['pby'],
			pc = payload['pc'],
			pcx = payload['pcx'],
			pcy = payload['pcy'],
			pd = payload['pd'],
			pdx = payload['pdx'],
			pdy = payload['pdy'],
			pe = payload['pe'],
			pex = payload['pex'],
			pey = payload['pey'],
			pf = payload['pf'],
			pfx = payload['pfx'],
			pfy = payload['pfy'],
			pg = payload['pg'],
			pgx = payload['pgx'],
			pgy = payload['pgy'],
			ph = payload['ph'],
			phx = payload['phx'],
			phy = payload['phy'],
			pi = payload['pi'],
			pix = payload['pix'],
			piy = payload['piy'],
			pj = payload['pj'],
			pjx = payload['pjx'],
			pjy = payload['pjy'],
			pk = payload['pk'],
			pkx = payload['pkx'],
			pky = payload['pky'],
			pl = payload['pl'],
			plx = payload['plx'],
			ply = payload['ply'],
			pm = payload['pm'],
			pmx = payload['pmx'],
			pmy = payload['pmy'],
			pn = payload['pn'],
			pnx = payload['pnx'],
			pny = payload['pny'],
			po = payload['po'],
			pox = payload['pox'],
			poy = payload['poy'],
			pp = payload['pp'],
			ppx = payload['ppx'],
			ppy = payload['ppy'],
			pq = payload['pq'],
			pqx = payload['pqx'],
			pqy = payload['pqy'],
			pr = payload['pr'],
			prx = payload['prx'],
			pry = payload['pry']
			
		)
		s = self.Session()
		s.add(message)
		s.commit()


	#need to move to a helper function
	def TableCreator2(self,posetable):
		


		class PresenceTable(declarative_base()):
			__tablename__ = posetable
			uniquerowid = Column(Integer,
								 default=1,
								  primary_key=True
								 )
			frameid = Column(String)
			trackid = Column(String)
			delta = Column(String)
			deltatime = Column(String)
			pa = Column(String)
			pax = Column(String)
			pay = Column(String)
			pb = Column(String)
			pbx = Column(String)
			pby = Column(String)
			pc = Column(String)
			pcx = Column(String)
			pcy = Column(String)
			pd = Column(String)
			pdx = Column(String)
			pdy = Column(String)
			pe = Column(String)
			pex = Column(String)
			pey = Column(String)
			pf = Column(String)
			pfx = Column(String)
			pfy = Column(String)
			pg = Column(String)
			pgx = Column(String)
			pgy = Column(String)
			ph = Column(String)
			phx = Column(String)
			phy = Column(String)
			pi = Column(String)
			pix = Column(String)
			piy = Column(String)
			pj = Column(String)
			pjx = Column(String)
			pjy = Column(String)
			pk = Column(String)
			pkx = Column(String)
			pky = Column(String)
			pl = Column(String)
			plx = Column(String)
			ply = Column(String)
			pm = Column(String)
			pmx = Column(String)
			pmy = Column(String)
			pn = Column(String)
			pnx = Column(String)
			pny = Column(String)
			po = Column(String)
			pox = Column(String)
			poy = Column(String)
			pp = Column(String)
			ppx = Column(String)
			ppy = Column(String)
			pq = Column(String)
			pqx = Column(String)
			pqy = Column(String)
			pr = Column(String)
			prx = Column(String)
			pry = Column(String)
			

		return PresenceTable





#  CREATE TABLE staplespose5 (
#             uniquerowid SERIAL,
#             frameid VARCHAR,
#             trackid VARCHAR,
#             delta VARCHAR,
#             deltatime VARCHAR,
#             pa VARCHAR,
# 			pax VARCHAR,
# 			pay VARCHAR,
# 	 		pb VARCHAR,
# 			pbx VARCHAR,
# 			pby VARCHAR,
# 	 		pc VARCHAR,
# 			pcx VARCHAR,
# 			pcy VARCHAR,
# 	 		pd VARCHAR,
# 			pdx VARCHAR,
# 			pdy VARCHAR,
# 	 		pe VARCHAR,
# 			pex VARCHAR,
# 			pey VARCHAR,
# 	 		pf VARCHAR,
# 			pfx VARCHAR,
# 			pfy VARCHAR,
# 	 		pg VARCHAR,
# 			pgx VARCHAR,
# 			pgy VARCHAR,
# 	 		ph VARCHAR,
# 			phx VARCHAR,
# 			phy VARCHAR,
# 	 		pi VARCHAR,
# 			pix VARCHAR,
# 			piy VARCHAR,
# 	 		pj VARCHAR,
# 			pjx VARCHAR,
# 			pjy VARCHAR,
# 	 		pk VARCHAR,
# 			pkx VARCHAR,
# 			pky VARCHAR,
# 	 		pl VARCHAR,
# 			plx VARCHAR,
# 			ply VARCHAR,
# 	 		pm VARCHAR,
# 			pmx VARCHAR,
# 			pmy VARCHAR,
# 	 		pn VARCHAR,
# 			pnx VARCHAR,
# 			pny VARCHAR,
# 	 		po VARCHAR,
# 			pox VARCHAR,
# 			poy VARCHAR,
# 	 		pp VARCHAR,
# 			ppx VARCHAR,
# 			ppy VARCHAR,
# 	 		pq VARCHAR,
# 			pqx VARCHAR,
# 			pqy VARCHAR,
# 	 		pr VARCHAR,
# 			prx VARCHAR,
# 			pry VARCHAR
# 	)
	 
	 
	 
	 
	 
	 
	 	



