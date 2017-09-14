from tweepy.streaming import StreamListener
import json


class FreqListener(StreamListener):
    """FreqListener extends StreamListener."""

    def __init__(self, db_handler, table_name, unpacker_function, api):
        self.db_handler = db_handler
        self.table_name = table_name
        self.unpacker = unpacker_function
        self.api = api

    '''
    def on_data(self, data):
        try:
            # print(json.loads(data)['text'])
            tweet = json.loads(data)
            self.db_handler.insert_tweet(self.table_name, tweet, self.unpacker)
            return True
        except BaseException as e:
            print("Error on_data: {}".format(str(e)))
        return True
    '''

    def on_status(self, status):
        try:
            # print(json.loads(data)['text'])
            print("status")
            print(status.text)
            self.db_handler.insert_tweet(self.table_name, status, self.unpacker)
        except BaseException as e:
            print("Error on_status: {}".format(str(e)))

    def on_error(self, status_code):
        print('ERROR:', status_code)
        if status_code == 420:
            return False
        return True

