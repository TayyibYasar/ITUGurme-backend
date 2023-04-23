from bs4 import BeautifulSoup
from flask import current_app
import requests

def Scrape(dates):
    for date in dates:
        print(date) 
        for i in range(2):
            if(i == 0):
                print("Ogle yemegi")
                islunch = True
                tip = "itu-ogle-yemegi-genel"
            else:
                print("Aksam yemegi")
                islunch = False
                tip = "itu-aksam-yemegi-genel"
            site = "https://sks.itu.edu.tr/ExternalPages/sks/yemek-menu-v2/uzerinde-calisilan/yemek-menu.aspx?tip=" + tip + "&&value=" + date
            r = requests.get(site)
            soup = BeautifulSoup(r.content, "html.parser")
            gelen_veri = soup.find_all("a",{"class":"js-nyro-modal"})
            food_names = []
            for yemek in gelen_veri:
                if yemek.text:
                    if(len(yemek.text) > 40):
                        food_names.append(yemek.text.split("(")[0])
                        continue
                    food_names.append(yemek.text) 
            db = current_app.config["db"]
            date_ = date.split("-")
            date_ = date_[1] + "-" + date_[0] + "-" + date_[2]
            if(len(food_names) > 0):   
                db.insert_food(food_names, islunch, date_)
            for yemek in food_names:
                print(yemek)
            if(len(food_names) == 0):
                print("Yemek yok")
            #delete past_meals
