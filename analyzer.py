import time


def coin_count(handler, table_name):
    btc_count = handler.query_list("SELECT SUM(retweets) FROM {} WHERE coins LIKE '%btc%'".format(table_name))[0][0] \
                  + handler.query_list("SELECT COUNT(*) FROM {} WHERE coins LIKE '%btc%'".format(table_name))[0][0]
    eth_count = handler.query_list("SELECT SUM(retweets) FROM {} WHERE coins LIKE '%eth%'".format(table_name))[0][0] \
                  + handler.query_list("SELECT COUNT(*) FROM {} WHERE coins LIKE '%eth%'".format(table_name))[0][0]
    xrp_count = handler.query_list("SELECT SUM(retweets) FROM {} WHERE coins LIKE '%xrp%'".format(table_name))[0][0] \
                  + handler.query_list("SELECT COUNT(*) FROM {} WHERE coins LIKE '%xrp%'".format(table_name))[0][0]
    return \
        '{} people tweeted about Bitcoin in the past 24 hours. #btc #crypto'.format(btc_count), \
        '{} people tweeted about Ethereum in the past 24 hours. #eth #crypto'.format(eth_count), \
        '{} people tweeted about Ripple in the past 24 hours. #xrp #crypto'.format(xrp_count)


def analyzer(handler, table_name):
    """Analyzer thread function"""
    # TODO: IMPLEMENT ANALYZER ON ANALYZER.PY
    print("Handler: running, simulating my DB work for 5 seconds.")
    print(coin_count(handler, table_name))
    time.sleep(5)  # Need to sleep so timer does not get messed up.
    print("Handler: told main Im done with my work")
