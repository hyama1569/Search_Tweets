import os
from datetime import datetime
import time
import math
from requests_oauthlib import OAuth1Session
from pytz import timezone
from dateutil import parser
import csv
import json
import sys

##################################################################

CONSUMER_KEY = "YOUR CONSUMER_KEY"
CONSUMER_SECRET = "YOUR CONSUMER_SECRET"
ACCESS_TOKEN = "YOUR ACCESS_TOKEN"
ACCESS_SECRET = "YOUR ACCESS_SECRET"

##################################################################

SEARCH_TWEETS_URL = 'https://api.twitter.com/1.1/search/tweets.json'
RATE_LIMIT_STATUS_URL = "https://api.twitter.com/1.1/application/rate_limit_status.json"
SEARCH_LIMIT_COUNT = 100


def get_twitter_session():
    return OAuth1Session(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)


def search_twitter_timeline(keyword, keyword_url, max_id='', since_id='', until=''):
    timelines = []
    nx_max_id = ''
    twitter = get_twitter_session()
    if len(keyword_url) != 0:
        keyword = keyword + ' OR ' + keyword_url
    params = {'q': keyword, 'count': SEARCH_LIMIT_COUNT, 'result_type': 'recent'}

    if max_id != '':
        params['max_id'] = max_id
    if since_id != '':
        params['since_id'] = since_id
    if until != '':
        params['until'] = until

    print(params)

    req = twitter.get(SEARCH_TWEETS_URL, params=params)

    if req.status_code == 200:
        search_timeline = json.loads(req.text)

        for tweet in search_timeline['statuses']:
            if(tweet['id'] == max_id):
                continue
            nx_max_id = tweet['id']
            timeline = []
            
            timeline.append(tweet['id'])
            timeline.append(tweet['user']['screen_name'])
            #timeline.append(tweet['user']['name'])
            timeline.append(tweet['user']['id'])
            timeline.append(tweet['user']['description'])
            timeline.append(tweet['user']['statuses_count'])
            timeline.append(tweet['user']['followers_count'])
            timeline.append(tweet['user']['friends_count'])
            timeline.append(tweet['user']['favourites_count'])
            timeline.append(tweet['user']['listed_count']) 
            jst_string = str(parser.parse(tweet['created_at']).astimezone(timezone('Asia/Tokyo')))
            date = datetime.strptime(jst_string,'%Y-%m-%d %H:%M:%S%z')
            timeline.append("{year}/{month}/{date} {hour}:{minute}:{second}".format(year=date.year,month=date.month,date=date.day,hour=date.hour,minute=date.minute,second=date.second))
            timeline.append(tweet['text'])
            timeline.append(tweet['in_reply_to_status_id'])
            timeline.append(tweet['in_reply_to_user_id'])
            timeline.append(tweet['in_reply_to_screen_name'])
            timeline.append(tweet['retweet_count'])        
            timeline.append(tweet['favorite_count'])
            
            if 'retweeted_status' in tweet:
                retweeted_tweet = tweet['retweeted_status']
                timeline.append("TRUE")
                timeline.append(retweeted_tweet['id'])
                timeline.append(retweeted_tweet['user']['screen_name'])
                #timeline.append(retweeted_tweet['user']['name'])
                timeline.append(retweeted_tweet['user']['id'])
                timeline.append(retweeted_tweet['user']['description'])
                timeline.append(retweeted_tweet['user']['statuses_count'])
                timeline.append(retweeted_tweet['user']['followers_count']) 
                timeline.append(retweeted_tweet['user']['friends_count']) 
                timeline.append(retweeted_tweet['user']['favourites_count']) 
                timeline.append(retweeted_tweet['user']['listed_count']) 
                jst_string = str(parser.parse(retweeted_tweet['created_at']).astimezone(timezone('Asia/Tokyo')))
                date = datetime.strptime(jst_string,'%Y-%m-%d %H:%M:%S%z')
                timeline.append("{year}/{month}/{date} {hour}:{minute}:{second}".format(year=date.year,month=date.month,date=date.day,hour=date.hour,minute=date.minute,second=date.second))
                #timeline.append(retweeted_tweet['text'])
                timeline.append(retweeted_tweet['retweet_count'])        
                timeline.append(retweeted_tweet['favorite_count'])
            else:
                timeline.append("FALSE")
                timeline.append('')
                timeline.append('')
                #timeline.append('')
                timeline.append('')
                timeline.append('')
                timeline.append('') 
                timeline.append('') 
                timeline.append('') 
                timeline.append('') 
                timeline.append('') 
                timeline.append('')
                #timeline.append('')
                timeline.append('')        
                timeline.append('')     
                
            if 'quoted_status' in tweet:
                quoted_tweet = tweet['quoted_status']
                timeline.append("TRUE")
                timeline.append(quoted_tweet['id'])
                timeline.append(quoted_tweet['user']['screen_name'])
                #timeline.append(quoted_tweet['user']['name'])
                timeline.append(quoted_tweet['user']['id'])
                timeline.append(quoted_tweet['user']['description'])
                timeline.append(quoted_tweet['user']['statuses_count'])
                timeline.append(quoted_tweet['user']['followers_count']) 
                timeline.append(quoted_tweet['user']['friends_count']) 
                timeline.append(quoted_tweet['user']['favourites_count']) 
                timeline.append(quoted_tweet['user']['listed_count']) 
                jst_string = str(parser.parse(quoted_tweet['created_at']).astimezone(timezone('Asia/Tokyo')))
                date = datetime.strptime(jst_string,'%Y-%m-%d %H:%M:%S%z')
                timeline.append("{year}/{month}/{date} {hour}:{minute}:{second}".format(year=date.year,month=date.month,date=date.day,hour=date.hour,minute=date.minute,second=date.second))
                timeline.append(quoted_tweet['text'])
                timeline.append(quoted_tweet['retweet_count'])        
                timeline.append(quoted_tweet['favorite_count'])
            else:
                timeline.append("FALSE")
                timeline.append('')
                timeline.append('')
                #timeline.append('')
                timeline.append('')
                timeline.append('')
                timeline.append('') 
                timeline.append('') 
                timeline.append('') 
                timeline.append('') 
                timeline.append('') 
                timeline.append('')
                timeline.append('')
                timeline.append('')        
                timeline.append('')          
            timelines.append(timeline)
            
    else:
        print("ERROR1: %d" % req.status_code)
        sys.exit()
        
    twitter.close()

    return timelines, nx_max_id


def get_rate_limit_status():
    twitter = get_twitter_session()
    req = twitter.get(RATE_LIMIT_STATUS_URL)
    if req.status_code == 200:
        limit_api = json.loads(req.text)

        limit = limit_api['resources']['search']['/search/tweets']['limit']
        remaining = limit_api['resources']['search']['/search/tweets']['remaining']
        reset = limit_api['resources']['search']['/search/tweets']['reset']
        reset_minute = math.ceil((reset - time.mktime(datetime.now().timetuple())) / 60)
        applimit = limit_api['resources']['application']['/application/rate_limit_status']['limit']
        appremaining = limit_api['resources']['application']['/application/rate_limit_status']['remaining']
        appreset = limit_api['resources']['application']['/application/rate_limit_status']['reset']
        appreset_minute = math.ceil((appreset - time.mktime(datetime.now().timetuple())) / 60)
        
        twitter.close()
        return limit, remaining, reset_minute, applimit, appremaining, appreset_minute
    
    else:
        print("ERROR2: %d" % req.status_code)
        sys.exit()
    

def check_api_remain_and_sleep():
    limit, remaining, reset_minute, applimit, appremaining, appreset_minute = get_rate_limit_status()
    print('-' * 30)
    #print('limit :{}'.format(limit))
    print('remaining :{}'.format(remaining))
    print('reset :{} minutes'.format(reset_minute))
    print('appremaining :{}'.format(appremaining))
    print('appreset :{} minutes'.format(appreset_minute))

    if remaining == 1:
        print('sleep {} minutes'.format((int(reset_minute) + 1)))
        time.sleep(60 * (int(reset_minute) + 1))
        return
    if appremaining == 1:
        print('sleep {} minutes'.format((int(appreset_minute) + 1)))
        time.sleep(60 * (int(appreset_minute) + 1))
        return

    return


def write_tweet_to_file(timelines, keyword):
    try:
        with open(keyword + ".csv", mode='a', encoding='Shift_JIS', errors='ignore', newline='') as f:
            writer = csv.writer(f)
            for timeline in timelines:
                writer.writerow(timeline)

        return
    
    except Exception as e:
        with open(keyword + ".csv", mode='a', encoding='Shift_JIS', errors='ignore', newline='') as f:
            writer = csv.writer(f)
            for timeline in timelines:
                writer.writerow(timeline)

        return


def main():
    timelines = []
    keyword = ''
    keyword_url = ''
    max_id = ''
    since_id = ''
    until = ''
    
    header = ['id','screen_name', 'user_id','description','statuses_count','followers_count','friends_count',
              'favourites_count','listed_count','created_at','text','in_reply_to_status_id','in_reply_to_user_id',
              'in_reply_to_screen_name','retweet_count','favorite_count','is_retweet','retweeted_id',
             'retweeted_user_screen_name', 'retweeted_user_id','retweeted_user_description', 
              'retweeted_user_statuses_count','retweeted_user_followers_count', 'retweeted_user_friends_count', 
              'retweeted_user_favourites_count','retweeted_user_listed_count', 'retweeted_created_at','retweeted_retweet_count', 
              'retweeted_favorite_count','is_quoted','quoted_id', 'quoted_user_screen_name', 'quoted_user_id', 
              'quoted_user_description','quoted_user_statuses_count', 'quoted_user_followers_count', 
              'quoted_user_friends_count', 'quoted_user_favourites_count', 'quoted_user_listed_count', 'quoted_created_at', 
              'quoted_text','quoted_retweet_count','quoted_favorite_count']
    with open(keyword + ".csv", mode='w', encoding='Shift_JIS', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        

    while True:
        check_api_remain_and_sleep()

        timelines, max_id = search_twitter_timeline(keyword, keyword_url, max_id, since_id, until)

        time.sleep(1)

        if timelines == []:
            break

        write_tweet_to_file(timelines, keyword)
        
    
if __name__ == "__main__":
    main()
