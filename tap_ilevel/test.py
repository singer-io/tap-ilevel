# TESTING CONFIG
import os
import json
# TESTING: Set working directory and load config file from working dir (where config/state file are)
os.chdir('/Users/jeff.huth/Devel/tap-ilevel')
my_config_path = 'tap_config.json'
with open(my_config_path) as file:
    CONFIG = json.load(file)
CONFIG

from zeep import Client
from zeep.wsse.username import UsernameToken
client = Client(
    'https://sandservices.ilevelsolutions.com/DataService/Service/2019/Q1/DataService.svc?singleWsdl',
    wsse=UsernameToken(CONFIG['username'], CONFIG['password']))

get_folders_fn = client.bind(
    service='GetFolders',
    port='CustomBinding_IDataService2')
