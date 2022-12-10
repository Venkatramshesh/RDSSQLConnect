from sshtunnel import SSHTunnelForwarder
import sqlalchemy
import awswrangler as wr
import boto3
import os
from botocore.exceptions import ClientError
import pymysql

secret_name = "convention"
region_name = "us-east-1"
#access_key = os.getenv('accesskeyid')
#access_secret = os.environ.get('accesskeysecret')
# Create a Secrets Manager client
boto3.setup_default_session(profile_name='iamadmin-production')
client = boto3.client(service_name='secretsmanager') #, region_name=region_name,aws_access_key_id=access_key, aws_secret_access_key=access_secret)

try:
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    raise e

    # Decrypts secret using the associated KMS key.
secret = get_secret_value_response['SecretString']

with SSHTunnelForwarder(

    ('ec2-3-239-203-138.compute-1.amazonaws.com'),
    ssh_username='ec2-user',
    #allow_agent=False,
    ssh_pkey='c:/Users/venka/AWS/AWSkeys/Prod/A4L.pem',
    remote_bind_address=('aws-database-web.cax8xbchtiwa.us-east-1.rds.amazonaws.com', 3306)

) as tunnel:
    print("****SSH Tunnel Established****")

    db = pymysql.connect(
        db='convention',
        user = "admin",
        password = secret[13:29],
        host = "localhost",
        port=tunnel.local_bind_port
    )

    # create some test data first

    with db.cursor() as cur:
        cur.execute("drop table if exists test")
        cur.execute("create table test (name varchar(25), email varchar(25))")
        cur.execute("insert into test values('Venkat Ramshesh','venkatramshesh@yahoo.com')")


    db.commit()

    try:

        print("Getting data from thecatalog")
        s = ""
        s += "SELECT "
        s += "table_schema"
        s += ",table_name"
        s += " FROMinformation_schema.tables"
        s += " WHERE"
        s += " ("
        s += "table_schema= 'public'"
        s +=" )"
        s += " ORDER BYtable_schema,table_name;"


        # Now get the data in table test

        print("getting data from table test")
        with db.cursor() as cur:
            cur.execute("select * from test")
            for r in cur:
                print(r)
    finally:
        db.close()

    