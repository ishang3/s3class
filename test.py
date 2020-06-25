from s3client import S3Client
import pickle
import random


client = S3Client()


# this will help read the files at a given prefix 1/06232020
total = 0
objects = []
for file in client.get_all_s3_objects(Bucket='gap-warehouse', Prefix="1/06232020"):
    objects.append(file['Key'])
    total += 1
    print(total)

for key in objects:
    print(key)
    s3_object = client.client.get_object(Bucket='gap-warehouse', Key=key)
    data = s3_object['Body'].read()
    print(pickle.loads(data))




# def ret():
#     data = {
#     'timestamp': 1592952651,
#     'label': 'person',
#     'trackid' : random.randrange(100),
#     'left'    : random.randrange(100),
#     'right'    : random.randrange(100),
#     'top'    : random.randrange(100),
#     'bottom'    : random.randrange(100),
#     }
#     return data
#
#
#
#
#
# for x in range(400):
#     data = ret()
#     client.save_detection(data)