import boto3
import json
import decimal
from datetime import datetime
from foompus_utilities import *

#TODO validation rate_type ı da kontrol etmeli tekrardan validation fonksiyonu çağrılabilir
#TODO türkçe karakterler sıkıntı, unquote kullanıp kullanmaman gerektiğine karar ver.

dynamodb = boto3.client('dynamodb', region_name="eu-central-1")

def fetch_user(user):
    resp = dynamodb.query(
            TableName = "itugurme",
            IndexName = "currentMeal",
            KeyConditionExpression = "#open = :openPk",
            ExpressionAttributeNames = {'#open': "open"},
            ExpressionAttributeValues = {":openPk": {"S":"USER#" + user}}
        )
    return deserialize(resp['Items'])

def lambda_handler(event, context):
    body = event.get('body')
    if body is None:
        body = '{}'
    
    body = json.loads(body)
    validated, message = validate(body, ["text", "foodName"])

    if not validated:
        return response(400, message)
    
    currentDateAndTime = datetime.now()
    currentTime = currentDateAndTime.strftime("%H:%M:%S")
    currentDate = currentDateAndTime.strftime("%Y-%m-%d")
    
    user = event['requestContext']['authorizer']['user']

    comment = {}
    comment['text'] = body['text']
    comment['food_name'] = body['foodName']

    if(currentTime < '12'):
        islunch = 'L'
    else:
        islunch = 'D'

    #Gets current meal informations from open GSI.
    current_meal = get_current_meal(dynamodb, islunch, currentDate)
    
    #User activity on open meal.    
    current_user = fetch_user(user)
    
    transact_items = []

    food_data = None
    meal_metadata = None
    for meal_data in current_meal:
        if 'PK1' in meal_data:
            if meal_data['PK1'] == 'FOOD#' + comment['food_name']:
                food_data = meal_data
        else:
            meal_metadata = meal_data

    if food_data is None:
        return response(400, {"message": comment['food_name'] + " Food is not available for comment."})

    increment = 1

    #Check already comment.
    for data in current_user:
        if f'COMMENT#{user}#{comment["food_name"]}' in data['openSK']:
            increment = 0
            break
    
    transact_items.append({
        "Put": {
            "TableName":"itugurme",
            "Item" :{
                "PK": {"S":f"FOOD#{comment['food_name']}#{currentDate}"}, 
                "SK": {"S":f"COMMENT#{user}#{comment['food_name']}#{currentDate}"}, 
                "ts": {"S":"{}T{}".format(currentDate, currentTime)}, 
                "PK1": {"S":"FOOD#" + comment["food_name"]}, 
                "open":{"S":f"USER#{user}"},
                "openSK":{"S":f"COMMENT#{user}#{comment['food_name']}#{currentDate}"},
                "text_":{"S":str(comment['text'])},
                "likeCount":{"N":"0"},
                "dislikeCount":{"N":"0"},
                "average":{"N":"0"},
                "type_":{"S":"COMMENT"}
                }
            }
        })
    transact_items.append({
                "Update": {
                    "TableName":"itugurme",
                    "Key":{
                        "PK": {"S":meal_metadata['PK']},
                        "SK": {"S":f"{currentDate}#{comment['food_name']}"}
                    },
                    "UpdateExpression": "SET comment_count = comment_count + :p",
                    "ExpressionAttributeValues": {
                        ":p": { "N": str(increment) }
                    }
                }
            })
                
    try:
        resp = dynamodb.transact_write_items(
            TransactItems = transact_items
        )
    except Exception as e:
        print(e)
        return response(400, {'message': 'Transaction error...'})
    
    return response(resp['ResponseMetadata']['HTTPStatusCode'], {'message': 'Operation succesfull.'})
