import jwt
from foompus_utilities import *


def lambda_handler(event, context):
    token = event['Authorization']
    try:
        data = jwt.decode(token, "",algorithms="")
    except:
        return response(403,{'message':'Token is invalid or missing'})
    
    print(data)
    return response(200, {'message':'Hello World'})


