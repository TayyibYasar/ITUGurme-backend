from bs4 import BeautifulSoup
from datetime import datetime
import boto3
import urllib3

http = urllib3.PoolManager()
dynamodb = boto3.client('dynamodb', region_name="eu-central-1")
 
def lambda_handler(event, context):
    currentDateAndTime = datetime.now()
    currentTime = currentDateAndTime.strftime("%H:%M:%S")
    print("The current time is", currentTime)
    currentDate = currentDateAndTime.strftime("%Y-%m-%d")
    print("Current Date: ", currentDate)
    if(currentTime < '12'):
        print("Ogle yemegi")
        islunch = 'L'
    else:
        print("Aksam yemegi")
        islunch = 'D'
        
    site = "https://sks.itu.edu.tr/ExternalPages/sks/yemek-menu-v2/uzerinde-calisilan/yemek-menu.aspx"
    resp = http.request("GET", site)

    if resp.status == 200:
        soup = BeautifulSoup(resp.data, "html.parser")
        gelen_veri = soup.find_all("a",{"class":"js-nyro-modal"})
        food_names = []
        for yemek in gelen_veri:
            if yemek.text:
                if(len(yemek.text) > 40):
                    food_names.append(yemek.text.split("(")[0])
                    continue
                food_names.append(yemek.text) 

        dynamodb.put_item(
            TableName = "itugurme",
            Item = {
                "PK": {"S":f"MEAL#{islunch}#{currentDate}"}, 
                "SK": {"S":"METADATA"}, 
                "ts": {"S":"{}T{}".format(currentDate, currentTime)}, 
                "type_": {"S":"MEAL"}, 
                "average": {"N":"0"},
                "like_count": {"N": "0"},
                "dislike_count": {"N": "0"},
                "open":{"S":f"MEAL#{islunch}#{currentDate}"},
                "openSK":{"S":"METADATA"}
            }
        )

        for yemek in food_names:
            print(yemek)
            dynamodb.put_item(
                TableName = "itugurme",
                Item = {
                    "PK": {"S":f"MEAL#{islunch}#{currentDate}"}, 
                    "SK": {"S":f"{currentDate}#{yemek}"}, 
                    "ts": {"S":"{}T{}".format(currentDate, currentTime)}, 
                    "PK1": {"S":"FOOD#" + yemek}, 
                    "average": {"N":"0"},
                    "rate_count":{"N": "0"},
                    "comment_count":{"N": "0"},
                    "open":{"S":f"MEAL#{islunch}#{currentDate}"},
                    "openSK":{"S":f"{currentDate}#{yemek}"}
                }
            )
        if(len(food_names) == 0):
            print("Yemek yok")

