import io 
import os 
import requests
import zipfile
import logging 
import gzip 
import json
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Sequence
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import URL
import settings 
import datetime

engine = create_engine(URL(**settings.DATABASE), echo=True)
Base = declarative_base()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class User(Base):
	__tablename__ = 'users'
	#id_table = Column(Integer, Sequence('user_id_seq') ,primary_key=True)
	id_table = Column(Integer, primary_key=True)
	schema = Column(String)
	app = Column(String)
	groups = Column(String)
	amplitude_id = Column(String)
	session_id = Column(String)
	uuid  = Column(String)
	device_type = Column(String)
	adid = Column(String)
	idfa = Column(String)
	dma = Column(String)
	data = Column(String)
	user_properties = Column(String)
	event_properties = Column(String)
	device_carrier = Column(String)
	device_model = Column(String)
	device_family = Column(String)
	device_manufacturer = Column(String)
	device_brand = Column(String)
	platform = Column(String)
	os_name = Column(String)
	device_id = Column(String)
	start_version = Column(String)
	os_version = Column(String)
	processed_time = Column(String)
	city = Column(String)
	country = Column(String)
	region  = Column(String)
	language = Column(String)
	location_lng = Column(String)
	location_lat = Column(String)
	server_upload_time = Column(String)

	event_time = Column(String)
	client_event_time = Column(String)
	profess_time = Column(String)
	user_creation_time = Column(String)
	client_upload_time = Column(String)

	insert_id = Column(String)
	event_type = Column(String)
	event_id = Column(String)
	library = Column(String)
	amplitude_event_type = Column(String)
	version_name = Column(String)
	ip_address = Column(String)
	paying = Column(String)
	user_id = Column(String)

	def __str__(self):
		return str(self.id)


class WorkAPI():

	def __init__(self):
		pass

	def workthis(self,resp_var,path_to):
		unzip_this = zipfile.ZipFile(io.BytesIO(resp_var.content))
		unzip_this.extractall(path_to)
		sub_path = os.listdir(path_to)[0]
		LOD=[]
		for compressed in unzip_this.namelist():
			path_more = compressed.split('/')[1]
			#149402_2016-06-10_5#732.json.gz
			path_total = path_to + '/' + sub_path + '/' + path_more
			#ZipFiles/149402/149402_2016-06-10_5#732.json.gz
			path_extract = path_to + '/' + sub_path + '/'
			#ZipFiles/149402/
			with gzip.open(path_total, "rb") as f:
				for line in f:
					LOD.append(json.loads(line))
				for i in range(len(LOD)):
					dictionary = LOD[i]
					if '$schema' in dictionary.keys():
						dictionary['schema'] = dictionary.pop('$schema')
						dictionary['schema'] = str(dictionary['schema'])
					if '$insert_id' in dictionary.keys():
						dictionary['insert_id'] = dictionary.pop('$insert_id')	
					if 'groups' in dictionary.keys():
						dictionary['groups'] = str(dictionary['groups'])
					if 'app' in dictionary.keys():
						dictionary['app'] = str(dictionary['app'])
					if 'amplitude_id' in dictionary.keys():
						dictionary['amplitude_id'] = str(dictionary['amplitude_id'])
					if 'session_id' in dictionary.keys():
						dictionary['session_id'] = str(dictionary['session_id'])
					if 'event_properties' in dictionary.keys():
						dictionary['event_properties'] = str(dictionary['event_properties'])
					if 'data' in dictionary.keys():
						dictionary['data'] = str(dictionary['data'])
					if 'dma' in dictionary.keys():
						dictionary['dma'] = str(dictionary['dma'])
					if 'adid' in dictionary.keys():
						dictionary['adid'] = str(dictionary['adid'])
					if 'user_properties' in dictionary.keys():
						dictionary['user_properties'] = str(dictionary['user_properties'])
					if 'idfa' in dictionary.keys():
						dictionary['idfa'] = str(dictionary['idfa'])	
					ses.add(User(**dictionary))
					ses.commit()



Base.metadata.bind = engine        
Base.metadata.create_all()        
        
Session = sessionmaker(bind=engine)
ses = Session()    
API_KEY = settings.KEYS['API_KEY']
SECRET_KEY = settings.KEYS['SECRET_KEY']
path_to = 'ZipFiles'
work = WorkAPI()

#Start at the start date; and loop over for every 10 day increment 
start_date = datetime.date(2016, 1, 1)
delta_days = 5
final_date = datetime.date.today()
while start_date <= final_date:
	start = start_date.strftime("%Y%m%d")
	start_date += datetime.timedelta(days=delta_days)
	end = start_date.strftime("%Y%m%d")
	url_amp = "https://amplitude.com/api/2/export?start=%s&end=%s"%(start,end)
	response = requests.get(url_amp, auth=(API_KEY,SECRET_KEY))
	if (response.status_code == 200):
		work.workthis(response ,path_to)
	else:
		logger.info('*'*50)
		logger.info('Error 404 #ResponseNotFound')
	logger.info('+'*50)