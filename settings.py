from dotenv import load_dotenv
load_dotenv()

import os

TOKEN = os.getenv('AUTH_TOKEN')
APIURL = os.getenv('APIURL')
CTYPE = os.getenv('CTYPE')
ACCEPT = os.getenv('ACCEPT')

