"""
This application uses AWS S3 to add
and retreive data. This is meant to be used
as an instance within the pubsub code for iotcore.

"""

import boto3
import logging
import os
import pickle
import random
import datetime


class S3Client():

    def __init__(self):
        self.client = self._client()

    def save_detection(self, data, camid="1", warehouse=None):
        """
        Uploads message sent to iot Subscribe

        Bucket/CAMID/Date/Key

        :params data:(dictionary) metadata for the object detection
        :params warehouse:(string) name of warehouse
        :params camid:(string) id of camera sending message
        :returns: None
        """

        date = datetime.datetime.fromtimestamp(data['timestamp']).strftime('%m%d%Y')

        # each detection will have a unique id based off the timestamp and trackid
        uniqueid = str(data['timestamp']) + str(random.randrange(100000)) + '-' + \
                   data['label'] + '.txt'

        key = os.path.join(camid, date, uniqueid)
        self.client.put_object(Body=pickle.dumps(data), Bucket="gap-warehouse",
                               Key=key)

    def get_all_s3_objects(self, **base_kwargs):
        """
        Retrieves all objects within a certain s3 prefix


        :params Bucket:(string) Name of S3 bucket
        :params Prefix:(string) Name of respective s3 Prefix
        """
        continuation_token = None
        while True:
            list_kwargs = dict(MaxKeys=1000, **base_kwargs)
            if continuation_token:
                list_kwargs['ContinuationToken'] = continuation_token
            response = self.client.list_objects_v2(**list_kwargs)
            yield from response.get('Contents', [])
            if not response.get('IsTruncated'):  # At the end of the list?
                break
            continuation_token = response.get('NextContinuationToken')

        # https://gist.github.com/tamouse/b5c725082743f663fb531fa4add4b189
        # https://dzone.com/articles/boto3-amazon-s3-as-python-object-store

    def _client(self):
        """
        This will allow a connection with AWS s3 with the
        security credentials. Important to use 'aws configure' to
        set credentials.

        :returns: S3 Client
        """
        return boto3.client('s3')


