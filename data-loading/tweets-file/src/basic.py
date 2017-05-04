from __future__ import absolute_import, print_function

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

# Consumer key and secret 
consumer_key="YOUR_CONSUMER_KEY"
CONSUMER_secret="YOUR_CONSUMER_SECRET"

#  Access token 
access_token="YOUR_ACCESS_TOKEN"
access_token_secret="ACCESS_TOKEN_SECRET"

f = open('tweets.json', 'w')

class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream. """
    def on_data(self, data):
        print(data)
        f.write(data + '\n')
        return True

    def on_error(self, status):
        print(status)
        f.close()

if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    stream.filter(track=['elections', 'presidentielles', 'france'])

