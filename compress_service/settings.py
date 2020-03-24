import os
from dotenv import load_dotenv
from os.path import join, dirname

dotenv_path = join(dirname(__file__), '..', '.env')
load_dotenv(verbose=True)

FE_SERVICE_PORT = os.getenv('FE_SERVICE_PORT')
UC_SERVICE_PORT = os.getenv('UC_SERVICE_PORT')
HTTP_HOST_URL = os.getenv('HTTP_HOST_URL')
WS_HOST_URL = os.getenv('WS_HOST_URL')
WS_STOMP_URL = os.getenv('WS_STOMP_URL') if os.getenv('WS_STOMP_URL') else ""

UC_URL = f"{HTTP_HOST_URL}:{UC_SERVICE_PORT}"
FE_URL = f"{HTTP_HOST_URL}:{FE_SERVICE_PORT}"

MQ_EXCHANGE_KEY = os.getenv('MQ_EXCHANGE_KEY')
MQ_WS_PORT = os.getenv('MQ_WS_PORT')
MQ_HOST = os.getenv('MQ_HOST')
MQ_PORT = os.getenv('MQ_PORT')
MQ_USERNAME = os.getenv('MQ_USERNAME')
MQ_PASSWORD = os.getenv('MQ_PASSWORD')
MQ_VHOST = os.getenv('MQ_VHOST')
