import boto3

def delete_table(tableName):
    dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
    devices_table = dynamodb.Table(tableName)
    devices_table.delete()
    print("Table deleted.")


delete_table('itugurme')