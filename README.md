# Website Checker

There are two parts of this application:
- Producer - periodically checks the target websites
and sends results to Kafka instance 
- Consumer - reads website checks result from Kafka and saves them 
into Postgresql DB


## Requirements
- Python >= 3.8
- psycopg2 binaries (`sudo apt-get install psycopg2`)

## How to run
Install dependencies from the `requirements.txt`:
```commandline
pip3 install -r requirements.txt 
```
Set environment variables (alternatively pass these values as parameters, see Makefile targets):
```commandline
export WC_PG_URI="postgres://<user>:<password>@<address>:<port>/<scheme>?sslmode=require"
export WC_CONFIG_PATH=/path/to/config.json
export WC_KAFKA_URI="<address>:<port>"
export WC_KAFKA_SSL_PATH=/directory/with/ssl/files
```

Start Producer
```commandline
make producer_start
```

Start Consumer
```commandline
make consumer_start
```