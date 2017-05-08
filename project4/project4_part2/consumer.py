from kafka import KafkaConsumer
from textblob import TextBlob
import re
import nltk
from elasticsearch import Elasticsearch
import argparse
import ast
import time
from datetime import datetime
import random
es_obj = Elasticsearch()


lat_x = 18.0
lat_y = 71.0
long_x = -66.0
long_y = 170.0
def cleaned_tweet(val):
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w +:\ / \ / \S +)", " ", val).split())


def tweet_list(consumer,h_list):
    #mappings = {"tweet":{"properties":{"geo":{"properties":{"location":{"type": "geo_point"}}},"timestamp":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"message":{"type":"text" },"polarity":{"type":"integer"},"order_dict":{"properties":{"hashtag":{"type":"text"},"sentiment":{"type":"text"}}}}}}
        
    #es_obj.indices.create(index="example",body=mappings)

    for messages in consumer:
        print (messages)
        val = str(messages.value).split('::')
        cleaned_val = cleaned_tweet(val[0])
        ts = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(val[1],'%a %b %d %H:%M:%S +0000 %Y'))
        print (ts)
        sentiment_analysis(ts,[cleaned_val],h_list)        
    
def sentiment_analysis(time_s,tweet_list,h_list):
    sentiment_tweet_list = []
    for t in tweet_list:
        geo_lat = round(random.uniform(lat_x,lat_y),2)
        geo_long = round(random.uniform(long_x,long_y),2)
        dict_sentiment = {}
        dict_sentiment['text'] = t
        val = '' 
        for j in h_list: 
            print (j)
            temp = j[1:]
            print (temp)
            if temp in  t :
                print ('<----------isneide if ---------->',j)
                val = temp.lower()
                
        print ('<-------val-------->',val)
        sentiment_a = TextBlob(t)
        if sentiment_a.sentiment.polarity > 0 :
            sentiment_analysis = 'positive'
        elif sentiment_a.sentiment.polarity < 0 :
            sentiment_analysis = 'negative'
        else : 
            sentiment_analysis = 'neutral'
        #sentiment_tweet_list.append(dict_sentiment)

        #mappings = {"tweet":{"properties":{"geo":{"properties":{"location":{"type": "geo_point"}}},"timestamp":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"message":{"type":"text" },"polarity":{"type":"integer"},"order_dict":{"properties":{"hashtag":{"type":"text"},"sentiment":{"type":"text"}}}}}}
        
        #es_obj.indices.create(index="example",body=mappings)
        data = {}
        data['location']= {"lat":geo_lat,"lon":geo_long}
        data['text'] = t
        data['sentiment'] = sentiment_analysis
        data['hashtag'] = val
        data['timestamp'] =  time.time()
        
        #es_obj.indices.create(index='plotter_6',ignore=400, body=mappings)
        print (data)
        es_obj.index(index="example_1",doc_type='tweet',body=data)
        
    #count_tweets(sentiment_tweet_list)
             

def count_tweets(tweet_result):
    total_tweets = len(tweet_result)
    positive_tweets = 0
    negative_tweets = 0
    neutral_tweets = 0    
    for i in tweet_result:
        if i['sentiment_analysis']=='positive':
            positive_tweets = positive_tweets + 1
        elif i['sentiment_analysis']=='negative':
            negative_tweets = negative_tweets + 1
        else:
            neutral_tweets = neutral_tweets + 1
           
#    print ('positive tweets percent',positive_tweets/total_tweets)           
#    print ('negative tweets percent',negative_tweets/total_tweets)           
#    print ('neutral tweets percent',neutral_tweets/total_tweets)           
           
           
            
def main():
    #parser = argparse.ArgumentParser()
    #parser.add_argument('--hash_tags')
    #parser.add_argument('--topic')
    #args = parser.parse_args()
    #topic = args.topic
    h_list = ['#Trump','#Obama','#trump','#obama']
    consumer = KafkaConsumer("twitter_fetch_data")
    tweet_list(consumer,h_list)




if __name__ =='__main__':
    main()