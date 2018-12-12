from coinbase.wallet.client import Client
import os

class coinbase_api():

    def __init__(self):
        self.coinbase_key_id = os.environ['COINBASE_KEY_ID']
        self.coinbase_secret_key = os.environ['COINBASE_SECRET_KEY']
        self.coinbase_api_version = os.environ['COINBASE_API_VERSION']
        self.coinbase_conn = None

    def connect(self):
        """
        Set object attribute coinbase_conn with a live Coinbase API connection object.
        :return: None
        """
        self.coinbase_conn = Client(self.coinbase_key_id, self.coinbase_secret_key, api_version=self.coinbase_api_version)

    def get_historic_prices(self, currency, period):
        """
        Get BTC prices
        :param currency: The international currency that the BTC price will be cited in (e.g. 'USD').
        :param period: The historical time period to get BTC prices from.
        :return: JSON dump (if connection is live) or None (if no connection).
        """
        if self.coinbase_conn is not None:
            return self.coinbase_conn.get_historic_prices(currency=currency, period=period)
