import multiprocessing
import threading
import time
from keys import *
import tweepy
from listener import FreqListener
import db_handler as dbh
import re
import datetime


auth = tweepy.OAuthHandler(twitter_consumer_key, twitter_consumer_secret)
auth.set_access_token(twitter_access_token, twitter_access_secret)
api = tweepy.API(auth)


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

tweet_table_template = '(id_str VARCHAR(20) NOT NULL, created_at TIME WITH TIME ZONE, text TEXT NOT NULL, ' \
                       'user_info TEXT NOT NULL, retweets INTEGER, coins TEXT, language TEXT, lat REAL, long REAL, ' \
                       'witheld_in_countries TEXT, place TEXT)'


def _start_and_wait_analyzer():

    def analyzer_t():
        """Analyzer thread function"""
        print("Handler: running, simulating my DB work for 5 seconds.")
        time.sleep(5)
        print("Handler: told main Im done with my work")

    analyzer = threading.Thread(target=analyzer_t)
    analyzer.start()
    print("Main: waiting for analyzer to say it finished")
    analyzer.join()
    print("Main: analyzer says its done, lets start again!")


def _set_timer(hour=0, minute=0, second=0):
    """
    Blocks the calling thread until the time of the day specified by creating a timer and waiting for it to finish
    return. For example, _set_timer(hour=17) will block the calling function until 5:00pm. If its 4:00pm, it will
    return at 5:00pm of the same day, if its 6:00om it will return at 5:00pm of the next day.
    :param hour: hour of the day (0-23)
    :param minute: minute (0-59)
    :param second: second (0-59)
    :return: None
    """

    def null_function():
        return

    now = datetime.datetime.today()

    # Here we set the time of the day we want to wait until.
    target = now.replace(day=now.day, hour=hour, minute=minute, second=second)
    if target > now:
        delta_t = target - now
    else:
        delta_t = datetime.timedelta(days=1) - (now - target)

    secs = delta_t.seconds
    print(delta_t)
    t = threading.Timer(secs, null_function)
    t.start()
    t.join()


def run():
    """
    This is the manager thread that will manage the listener (who is the thread that listens for Twitter data and sends
    this data to our database. It will also manage the db_handler, which is the thread that does clean up on our
    database after every cycle so we limit the amount of memory stored in our database.  Check our their docs for more
    details.
    :return: None
    """

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
            unpacked_format += ',' + str(tweet.retweet_count)  # TODO make function to get actual number of retweets (Ale)

            # TODO make sure to ignore tweets that dont mention the coin explicitly implement get_coins

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
                # TODO: decide if we want to do something in case no coin is present on tweet.
                unpacked_format += ',NULL'
            else:
                print(coins_str)
                unpacked_list.append(coins_str)
                unpacked_format += ',%s'

            # lang
            if hasattr(tweet, 'lang'):
                unpacked_list.append(tweet.lang)
                unpacked_format += ',%s'
            else:
                unpacked_format += ',NULL'

            # coordinates
            if hasattr(tweet.coordinates, 'coordinates'):
                unpacked_format += ',' + str(tweet.coordinates.coordinates[1])  # lat
                unpacked_format += ',' + str(tweet.coordinates.coordinates[0])  # long
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
                unpacked_list.append(str(tweet.place))
                unpacked_format += ',%s'
            else:
                unpacked_format += ',NULL'

            unpacked_format += ')'
            return unpacked_format, unpacked_list

        stream = tweepy.Stream(auth, FreqListener(_db_handler, _table_name, unpacker, api,))
        stream.filter(track=filters)


    # Pre initialization
    # timer_lock = threading.Event()
    my_db_handler = dbh.Handler('cryptweets_test', 'lfvarela')
    i = 0

    while True:
        table_name = 'tweets_day_' + str(i)
        print("Main: creating table")

        with my_db_handler:
            my_db_handler.create_table(table_name, tweet_table_template)

            listener = multiprocessing.Process(target=twitter_listener_t, args=(my_db_handler, table_name,))
            listener.start()

            print("Main: Simulating one day")
            _set_timer(hour=17, minute=0)

            print("Main: terminating listener")
            listener.terminate()
            print("Main: Listener terminated")
            listener.join()
            print("Main: telling analyzer to go")
            _start_and_wait_analyzer()

        i += 1


if __name__ == '__main__':
    run()
