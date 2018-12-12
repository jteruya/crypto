from coinbase_api import coinbase_api
from s3_api import s3_api
import json
import os
from datetime import datetime

# Establish Coinbase Connection
coinbase = coinbase_api()
coinbase.connect()

# Open output file connection
tempfile_dirname = os.getenv("HOME") + '/crypto/data/'
tempfile_name = 'platypus_coinbase_pull_{}.json'.format(datetime.now().strftime("%Y%m%d%H%M%S"))
tempfile_path = tempfile_dirname + tempfile_name
output_file = open(tempfile_path, "w+")

# Using the coinbase connection object, get the historical prices
output_file.write(json.dumps(coinbase.get_historic_prices("USD", "day")))

# Load file to S3.
bucket_name = 'platypus-coinbase-data'
s3 = s3_api()
s3.connect()
s3.upload_file(bucket_name, tempfile_path)

# Remove local version of output file.
os.remove(tempfile_path)

# Close output file connection.
output_file.close()
