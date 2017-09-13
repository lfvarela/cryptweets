from tweepy.streaming import StreamListener
import json


class FreqListener(StreamListener):
    """FreqListener extends StreamListener."""

    def on_data(self, data):
        try:
            print(json.loads(data)['text'])
            return True
        except BaseException as e:
            print("Error on_data: {}".format(str(e)))
        return True

    def on_error(self, status_code):
        print('ERROR:', status_code)
        if status_code == 420:
            return False
        return True

