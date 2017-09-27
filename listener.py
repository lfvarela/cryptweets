import re
from FreqListener import FreqListener
import tweepy
from keys import *

# TODO: consolidate these
# NOTE: These three data structures are completely dependent on each other.
# filters: all filters for tweepy.
# coins: coin symbols in filters.
# symbol_map: all filters that represent a coin.
filters = ['bitcoin', 'ethereum', 'btc', 'eth', 'ripple', 'xrp']
coins = ['eth', 'btc', 'xrp']
symbols_map = {
    'bitcoin': 'btc',
    'ethereum': 'eth',
    'ripple': 'xrp'
}


auth = tweepy.OAuthHandler(twitter_consumer_key, twitter_consumer_secret)
auth.set_access_token(twitter_access_token, twitter_access_secret)
api = tweepy.API(auth)


def twitter_listener_t(_db_handler, _table_name):
    """Listener thread function"""

    def unpacker(tweet):
        unpacked_list = []
        unpacked_format = '('

        # id_str
        unpacked_list.append(tweet.id_str)  # id_str TEXT
        unpacked_format += '%s'

        # created_at
        unpacked_list.append(re.search('[0-9]{2}:[0-9]{2}:[0-9]{2}',
                                       str(tweet.created_at)).group(0) + ' +0000')  # create_at TIME WITH TIME ZONE
        unpacked_format += ',%s'

        # text
        unpacked_list.append(tweet.text)
        unpacked_format += ',%s'

        def to_key_repr(string):
            return '\'' + string + '\'' + ':'

        def to_str_repr(string):
            return '\'' + string + '\''

        # user info (dict as text)
        user_str = '{'
        user_str += to_key_repr('id') + tweet.user.id_str + ','
        user_str += to_key_repr('name') + to_str_repr(tweet.user.name) + ','
        user_str += to_key_repr('screen_name') + to_str_repr(tweet.user.screen_name) + ','
        user_str += to_key_repr('followers_count:') + str(tweet.user.followers_count) + ','
        user_str += to_key_repr('verified:') + str(tweet.user.verified)
        user_str += '}'
        unpacked_list.append(user_str)
        unpacked_format += ',%s'

        # retweet_count
        unpacked_format += ',' + str(tweet.retweet_count)

        # coins
        coins_str = ''
        coins_matched = []
        for filter_ in filters:
            match_obj = re.search(filter_, tweet.text, re.IGNORECASE)
            if match_obj:
                if filter_ in coins:
                    if filter_ not in coins_matched:
                        coins_matched.append(filter_)
                        coins_str += filter_ + ','
                elif filter_ in symbols_map:
                    if filter_ not in coins_matched:
                        coins_matched.append(symbols_map[filter_])
                        coins_str += symbols_map[filter_] + ','
        coins_str = coins_str.strip(',')
        if coins_str == '':
            unpacked_format += ',NULL'
            return False, '', ''
        else:
            unpacked_list.append(coins_str)
            unpacked_format += ',%s'

        # lang
        if hasattr(tweet, 'lang'):
            unpacked_list.append(tweet.lang)
            unpacked_format += ',%s'
        else:
            unpacked_format += ',NULL'

        # coordinates
        if tweet.coordinates:
            unpacked_format += ',' + str(tweet.coordinates['coordinates'][1])  # lat
            unpacked_format += ',' + str(tweet.coordinates['coordinates'][0])  # long
        else:
            unpacked_format += ',NULL,NULL'

        # withheld_in_countries
        if hasattr(tweet, 'withheld_in_countries'):
            unpacked_list.append(str(tweet.withheld_in_countries))
            unpacked_format += ',%s'
        else:
            unpacked_format += ',NULL'

        # place
        if tweet.place:
            unpacked_list.append(tweet.place.full_name)
            unpacked_format += ',%s'
        else:
            unpacked_format += ',NULL'

        unpacked_format += ')'
        return True, unpacked_format, unpacked_list

    stream = tweepy.Stream(auth, FreqListener(_db_handler, _table_name, unpacker, api, ))
    stream.filter(track=filters)


def post_tweet(text):
    api.update_status(text)
