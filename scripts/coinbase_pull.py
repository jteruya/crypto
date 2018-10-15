from coinbase.wallet.client import Client
import json
import boto3
import os
import psycopg2
import psycopg2.extras
from datetime import datetime

# Coinbase connection credentials
coinbase_key_id = os.environ['COINBASE_KEY_ID']
coinbase_secret_key = os.environ['COINBASE_SECRET_KEY']
coinbase_api_version = os.environ['COINBASE_API_VERSION']

# Create a coinbase connection object
cb_client = Client(coinbase_key_id, coinbase_secret_key, api_version=coinbase_api_version)

# Open output file connection
ofile_root_dirname = '/home/ec2-user/'
#ofile_root_dirname = '/Users/jteruya/'
ofile_app_location = 'crypto/data/'
ofile_name = 'platypus_coinbase_pull_{}.json'.format(datetime.now().strftime("%Y%m%d%H%M%S"))
ofile_absolute_path = ofile_root_dirname + ofile_app_location + ofile_name
ofile = open(ofile_absolute_path, "w+")

# Using the coinbase connection object, get the historical prices
ofile.write(json.dumps(cb_client.get_historic_prices(currency = 'USD', period = 'day')))

# Close output file connection
ofile.close()

# Load file to S3
bucket_name = 'platypus-coinbase-data'
s3 = boto3.resource('s3')
s3.meta.client.upload_file(ofile_absolute_path, bucket_name, ofile_name)

# Redshift connection credentials
redshift_host = os.environ['REDSHIFT_HOST']
redshift_db_name = os.environ['REDSHIFT_DB_NAME']
redshift_port = os.environ['REDSHIFT_PORT']
redshift_user = os.environ['REDSHIFT_USER']
redshift_password = os.environ['REDSHIFT_PASSWORD']

# Open Redshift Connection
conn = psycopg2.connect(host=redshift_host, dbname=redshift_db_name, port=redshift_port, user=redshift_user, password=redshift_password)
cursor = conn.cursor()

# Open JSON, Parse and Load
with open(ofile_absolute_path) as f:
    data = json.load(f)
    prices = data['prices']
    values = []
    for price_element in prices:
        price = price_element['price']
        time = datetime.strptime(price_element['time'], '%Y-%m-%dT%H:%M:%SZ')
        values.append((price, time))
    psycopg2.extras.execute_values(cursor, "INSERT INTO prices (price, time) VALUES %s", values)
conn.commit()

# Close Redshift Connection
conn.close()

# Remove local file
os.remove(ofile_absolute_path)




