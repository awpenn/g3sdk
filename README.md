# firsttime setup
- virtualenv .venv
- source .venv/bin/activate
- pip install gen3 pandas

- create .env file with following variables:
    - AUTH_TOKEN (for DSS)
    - APIURL (for DSS up to .../api/)
    - CTYPE = "application/json"
    - ACCEPT = "application/json"

- download datastage API 'credentials.json' and place in root folder