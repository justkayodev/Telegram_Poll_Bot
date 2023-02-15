import os
import json
import requests
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

load_dotenv()
cwd = os.getenv("PROJECT_DIR")

#logging.basicConfig(filename='{}/app.log'.format(cwd), format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

today = datetime.today()

def fetch_image(image_url, image_loc, image_name):
    logging.info("Making the call to download the image.")
    try:
        resp = requests.get(image_url, allow_redirects=True)
        with open("{}{}.png".format(os.path.join(cwd, image_loc, image_name), today.strftime("%Y-%m-%d")), 'wb') as outfile:
            outfile.write(resp.content)
    except Exception as e:
        logging.error("Unable to download the image. Error - {}".format(e))
        return {"Status": "Failure"}
    return {"Status": "Success"}


def pull_data(database_id):
    url = "https://api.notion.com/v1/databases/{}/query".format(database_id)
    headers = {
        "Authorization": "Bearer " + os.getenv("NOTION_TOKEN"),
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }

    payload = {
        "filter": {
            "property": "Date",
            "date": {
                "equals": today.date().strftime("%Y-%m-%d")
            }
        }
    }

    logging.info("Making the api call to pull locations data from notion.")
    try:
        resp = requests.post(url, json=payload, headers=headers)
        logging.info("Response from the api call to pull locations db data - {}".format(resp.json()))
        resp_json = resp.json()

        if resp.status_code == 200:
            if len(resp_json["results"]) > 1:
                logging.error("Multiple location entries found in locations db for today.")
                return {"Status": "Failure"}
            elif len(resp_json["results"]) == 0:
                logging.error("No entry found for the location of today in DB")
                return {"Status": "Failure"}
        else:
            return {"Status": "Failure"}

        image_url = resp_json["results"][0]["properties"]["Images"]["files"][0]["file"]["url"]
    except Exception as e:
        logging.error("Unable to pull image data for today from locations DB. Error - {}".format(e))
        return {"Status": "Failure"}

    return {"Status": "Success", "URL": image_url}


def initiate_data_pull():
    database_id = os.getenv("IMAGES_DB_ID")
    image_loc = os.getenv("IMAGE_LOCATION")
    image_name = os.getenv("IMAGE_NAME")

    data = pull_data(database_id)
    if "URL" in data:
        response = fetch_image(data["URL"], image_loc, image_name)
    else:
        logging.error("Unable to get the url of the image from the locatons database. Thus exiting the flow.")
        return {"Status": "Failure"}

    return response