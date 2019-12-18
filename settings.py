from dotenv import load_dotenv
load_dotenv()

import os

TOKEN = os.getenv('AUTH_TOKEN')
APIURL = os.getenv('APIURL')
CTYPE = os.getenv('CTYPE')
ACCEPT = os.getenv('ACCEPT')

HEADERS = {
    'Content-Type': CTYPE,
    'Accept': ACCEPT,
    "Authorization": "Bearer {token}".format(token=TOKEN),
    'User-Agent': "PostmanRuntime/7.18.0",
    'Cache-Control': "no-cache",
    'Postman-Token': "dd5ae43f-70d8-41e7-8911-55f1d3fa4d6d,0a7e9fcf-becc-49a0-9e86-7afa02e425d5",
    'Host': "dev3.niagads.org",
    'Accept-Encoding': "gzip, deflate",
    'Connection': "keep-alive",
    'cache-control': "no-cache"
}