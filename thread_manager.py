import multiprocessing
import threading
import db_handler as dbh
import datetime
import listener
import analyzer

tweet_table_template = '(id_str VARCHAR(20) NOT NULL, created_at TIME WITH TIME ZONE, text TEXT NOT NULL, ' \
                       'user_info TEXT NOT NULL, retweets INTEGER, coins TEXT, language TEXT, lat REAL, long REAL, ' \
                       'witheld_in_countries TEXT, place TEXT)'


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
    print('Time until next tweet: ' + str(delta_t))
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

    # Pre initialization
    my_db_handler = dbh.Handler('cryptweets_test', 'lfvarela')
    i = 0

    while True:
        table_name = 'tweets_day_' + str(i)
        print('Main: creating table ' + str(table_name))

        with my_db_handler:
            my_db_handler.create_table(table_name, tweet_table_template)

            listener_t = multiprocessing.Process(target=listener.twitter_listener_t, args=(my_db_handler, table_name,))
            listener_t.start()
            _set_timer(hour=17)

            print("Main: terminating listener")
            listener_t.terminate()
            listener_t.join()
            print("Main: analyzing data")
            analyzer.analyzer(my_db_handler, table_name)
            print()

        i += 1


if __name__ == '__main__':
    run()
