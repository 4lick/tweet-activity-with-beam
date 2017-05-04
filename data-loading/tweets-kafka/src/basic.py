from __future__ import absolute_import, print_function

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from kafka import KafkaProducer
from kafka.errors import KafkaError

import json
import datetime

# Consumer key and secret 
consumer_key="YOUR_CONSUMER_KEY"
CONSUMER_secret="YOUR_CONSUMER_SECRET"

#  Access token 
access_token="YOUR_ACCESS_TOKEN"
access_token_secret="ACCESS_TOKEN_SECRET"

# kafka producer
producer = KafkaProducer(bootstrap_servers=['kafka:9092'], value_serializer=lambda m: json.dumps(m).encode('utf-8'))

class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream. """    
    def on_data(self, data):
        try:
            self.c += 1
        except:
            self.c = 0
        if self.c % 10 == 0:
             print("%s - %d tweets sent" % (datetime.datetime.utcnow(), self.c))
        
        future = producer.send('tweets-topic', data)

        try:
            record_metadata = future.get(timeout=10)
        except KafkaError:
            # Decide what to do if produce request failed...
            log.exception()
            pass

        return True

    def on_error(self, status):
        print(status)
        
if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    stream.filter(track=['elections', 'presidentielles', 'france'])
    
