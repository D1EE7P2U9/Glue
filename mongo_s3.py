import pymongo
import pandas as pd


#mongo Creds
Mongo_String = '<<your-mongo-connection-string>>'
database = '<<DataBaseName>>'
collection = '<<Mongo-collection-Name>>'
 
mongo_client = pymongo.MongoClient(Mongo_String)
db = mongo_client[database]
collection = db[collection]


cursor = collection.find()
df = pd.DataFrame(list(cursor))

S3_Key = '<<s3_path_key>>'
Bucket_Name = '<<bucket_name>>'

s3_path = f"s3://{Bucket_Name}/{S3_Key}/{collection}/"
df.to_csv(s3_path, index=False)
