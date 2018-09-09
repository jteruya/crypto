from coinbase.wallet.client import Client
import json
import boto3
import os
from datetime import datetime

# Coinbase connection credentials# Coinb
coinbase_key_id = os.environ['COINBASE_KEY_ID']
coinbase_secret_key = os.environ['COINBASE_SECRET_KEY']
coinbase_api_version = os.environ['COINBASE_API_VERSION']

# Create a coinbase connection object
cb_client = Client(coinbase_key_id, coinbase_secret_key, api_version=coinbase_api_version)

# Open output file connection
ofile_root_dirname = '/home/ec2-user'
ofile_app_location = 'crypto/data/'
ofile_name = 'platypus_coinbase_pull_{}.json'.format(datetime.now().strftime("%Y%m%d%H%M%S"))
ofile_absolute_path = ofile_root_dirname + ofile_app_location + ofile_name
ofile = open(ofile_absolute_path, "w+")

# Using the coinbase connection object, get the historical prices
ofile.write(json.dumps(cb_client.get_historic_prices(currency = 'USD', period = 'all')))

# Close output file connection
ofile.close()

# Load file to S3
bucket_name = 'platypus-coinbase-data'
s3 = boto3.resource('s3')
s3.meta.client.upload_file(ofile_absolute_path, bucket_name, ofile_name)

# Remove local file
os.remove(ofile_absolute_path)
