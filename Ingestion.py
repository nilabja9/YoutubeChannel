import tweepy
from tweepy import OAuthHandler
import boto3
import json
import time
import twittercred
import awscred


if __name__ == '__main__':
    #Creating a Firehose Delivery Stream First
    #
    client = boto3.client('firehose', aws_access_key_id = awscred.accesskey, aws_secret_access_key = awscred.accesssecret)

    response = client.create_delivery_stream(
        DeliveryStreamName = 'kinesisnilabja',
        DeliveryStreamType = 'DirectPut',
        S3DestinationConfiguration = {
            'RoleARN': 'arn:aws:iam::626027370572:role/firehose_delivery_role',
            'BucketARN': 'arn:aws:s3:::gluebucketnilabja',
            'Prefix': 'nilabjatwitter042019',
            'BufferingHints': {
                'SizeInMBs': 5,
                'IntervalInSeconds': 120
            }

        }
    )

    print(response)

    #Wait for Firehose to be created
    time.sleep(60)

    #Authenticating Twitter
    auth = OAuthHandler(twittercred.consumerkey, twittercred.consumersecret)
    auth.set_access_token(twittercred.accesskey, twittercred.accesssecret)

    api = tweepy.API(auth)
    topic = 'Donald Trump'

    start_time = time.time()
    time_difference = 0
    tweets = []
    while time_difference < 120:
        tweets = tweepy.Cursor(api.search, q=topic).items(1000)
        for tweet in tweets:
            response = client.put_record(
                DeliveryStreamName='kinesisnilabja',
                Record={
                    'Data': json.dumps(tweet._json) + '\n'
                }


            )
            print("Collecting Tweet....")
            print(response)


        time.sleep(30)
        time_difference = int(time.time() - start_time)


    print("Waiting for 1 min more for the file to be delivered...")
    time.sleep(60)
    #Deleting the Firehose - After Work Done
    response = client.delete_delivery_stream(
        DeliveryStreamName='kinesisnilabja'
    )

    print(response)