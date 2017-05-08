from kafka import SimpleProducer, KafkaClient
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
from time import sleep
import argparse
import ast
parser = argparse.ArgumentParser()
parser.add_argument('--hash_tags')
parser.add_argument('--topic')
args = parser.parse_args()
topic = args.topic
kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka)
api_key = 'dgT5RVoEall7upyO5XcxDaWrI'
api_secret_key ='9Wrcgcqiiw7LBgRsuqzmJmTmYP5OEN4x2tL4OBha8QBgZsAwMz'  
token_key = '851843706512650240-mpmdGFkBGhKdaKnjAs9yAGkepCx1njY'
token_secret_key = 'Xhr7Ao9NJrfth851DaE7PYPRsQOXGgpHBPhRgui8qJI1Z' 


class StdOutListener(StreamListener):    
    def on_data(self, data):
        print ('<----------DATA------->',data)
        json_load = json.loads(data)
        print ('<-------JSON LOADS--------->',json_load)
        t = json_load['text']
        print ('<--------------t--------------->',t)
        codes = t.encode('utf-8')
        print (codes)
        
        time_s = json_load['created_at']  .encode('utf-8')      
        codes = codes +'::'+ time_s
        producer.send_messages("twitter_fetch_data", codes)
        sleep(2)
        return True
    def on_error(self, status):
        print status


def main():
    l = StdOutListener()
    auth = OAuthHandler(api_key, api_secret_key)
    auth.set_access_token(token_key,token_secret_key)
    stream = Stream(auth, l)
    #print (args.hash_tags)
    stream.filter(languages=["en"], track=['#Trump','#Obama','#trump','#obama'])

if __name__ == '__main__':
    main()