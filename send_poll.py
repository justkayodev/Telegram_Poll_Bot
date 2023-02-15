#!/bin/sh

import os
import io
import json
import requests
import logging
import re
import time
from google.cloud import vision
from google.cloud.vision_v1 import types
import pandas as pd
from pull_data import initiate_data_pull
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

load_dotenv()
cwd = os.getenv("PROJECT_DIR")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(cwd, "cloud_vision_api.json")

logging.basicConfig(filename='{}/app.log'.format(cwd), format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

today = datetime.today()

def send_location_img(image, caption, bot_token, channel_id):
    url = "https://api.telegram.org/bot{}/sendPhoto".format(bot_token)

    resp = {}
    params = {
        "chat_id": channel_id,
        "caption": caption
    }

    try:
        with open(image, 'rb') as infile:
            image_binary = infile.read()

        response = requests.post(url, params=params, files={"photo": image_binary})
        logging.info("Response from send image api call - {}".format(response.json()))
    except Exception as e:
        logging.error("Unable to send image to the chat. Error - {}".format(e))
        resp["Status"] = "Failure"

    resp["Status"] = "Success" if response.status_code == 200 else "Failure"
    return resp
    

def process_img(img, pattern):
    try:
        logging.info("Trying to process image and extract text information.")
        client = vision.ImageAnnotatorClient()
        with io.open(img, 'rb') as image_file:
            content = image_file.read()
        image = types.Image(content=content)
        response = client.text_detection(image=image)
        text = response.text_annotations[0].description
        location_ids = re.findall(pattern, text)
        logging.info("Found the location ids - {}".format(location_ids))
    except Exception as e:
        logging.error("Unable to process image. Error - {}".format(e))
    return location_ids


def register_poll(poll_data, notion_token, db_id):
    url = "https://api.notion.com/v1/pages"

    poll_id = poll_data["result"]["poll"]["id"]
    poll_date = datetime.now().astimezone(timezone.utc).today().date().isoformat()

    headers = {
        "Authorization": "Bearer " + notion_token,
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }

    data = {
        "Poll ID": {"title": [{"text": {"content": poll_id}}]},
        "Poll Date": {"date": {"start": poll_date, "end": None}},
        "Status": {"select": {"name": "Open"}}
    }

    payload = {
        "parent": {
            "database_id": db_id
            },
            "properties": data
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        logging.info("Registering the poll in notion. Response from the api call - {}".format(response.json()))
    except Exception as e:
        logging.error("Unable to register the poll. Error - {}".format(e))
        return {"Status": "Failure"}
    return {"Status": "Success"}

def register_events(poll_data, notion_token, location_ids, db_id):
    url = "https://api.notion.com/v1/pages"

    poll_id = poll_data["result"]["poll"]["id"]

    headers = {
        "Authorization": "Bearer " + notion_token,
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }

    data = {
        "Poll ID": {"title": [{"text": {"content": poll_id}}]},
        "Kayo Event 1": {"rich_text": [{"text": {"content": location_ids[0]}}]},
        "Kayo Event 2": {"rich_text": [{"text": {"content": location_ids[1]}}]},
        "Kayo Event 3": {"rich_text": [{"text": {"content": location_ids[2]}}]},
    }

    payload = {
        "parent": {
            "database_id": db_id
            },
            "properties": data
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        logging.info("Registered the poll events in notion. Response from the api call - {}".format(response.json()))    

        logging.info("Saving the location ids in the file...")
        poll_to_event_df = pd.read_csv(os.path.join(cwd, "poll_to_events.csv"), dtype=str)
        poll_to_event_df.loc[len(poll_to_event_df.index)] = [poll_id, location_ids[0], location_ids[1], location_ids[2]]
        poll_to_event_df.to_csv(os.path.join(cwd, "poll_to_events.csv"), index=False)
    except Exception as e:
        logging.error("Unable to register the poll events in notion or save in file. Error - {}".format(e))
        return {"Status": "Failure"}
    return {"Status": "Success"}


def send_poll(bot_token, channel_id, location_ids, poll_q, notion_token, poll_results_dbid, poll_to_event_dbid):
    poll_options_user = os.getenv("POLL_OPTIONS_USER")
    
    url = "https://api.telegram.org/bot{}/sendPoll".format(bot_token)

    params = {
        "chat_id": channel_id,
        "question": poll_q,
        "options": json.dumps(poll_options_user.split("*")),
        "is_anonymous": False
    }

    try:
        logging.info("Making the api call to send the poll in the chat.")
        resp = requests.get(url, data=params)
        json_resp = resp.json()
        logging.info("Response from send poll api call - {}".format(json_resp))

        if resp.status_code == 200:
            logging.info("Calling method to register the poll and events in notion DB.")
            register_poll(json_resp, notion_token, poll_results_dbid)
            register_events(json_resp, notion_token, location_ids, poll_to_event_dbid)
        else:
            return {"Status": "Failure"}

    except Exception as e:
        logging.error("Unable to send the poll in the chat. Error - {}".format(e))
        return {"Status": "Failure"}

    return {"Status": "Success", "Response": json_resp}
    

def main():
    img_name, img_loc = os.getenv("IMAGE_NAME"), os.getenv("IMAGE_LOCATION")
    locations_count, pattern = os.getenv("LOCATIONS_COUNT"), os.getenv("PATTERN")
    caption, poll_q = os.getenv("IMAGE_CAPTION"), os.getenv("POLL_Q")
    bot_token, channel_id = os.getenv("API_KEY"), os.getenv("CHANNEL_ID")
    wait_time, poll_duration = os.getenv("WAIT_TIME"), os.getenv("POLL_DURATION")
    venv, code_loc = os.getenv("VENV"), os.getenv("STOP_POLL_CODE")
    poll_results_dbid, notion_token, poll_to_event_dbid = os.getenv("POLL_RESULT_DB_ID"), os.getenv("NOTION_TOKEN"), os.getenv("POLL_TO_EVENT_DBID")


    data_pull_resp = initiate_data_pull()
    logging.info("Response from initiate pull method - {}".format(data_pull_resp))
    if data_pull_resp["Status"] != "Success":
        return data_pull_resp

    image = "{}{}.png".format(os.path.join(cwd, img_loc, img_name), today.strftime("%Y-%m-%d"))

    if (not os.path.exists(image)) or (data_pull_resp["Status"] != "Success"):
        logging.error("Image file not found. Exiting the flow....")
        return {"Status": "Failure"}
    
    location_ids = process_img(image, pattern)

    if len(location_ids) != int(locations_count):
        logging.error("Required locations count is not same as the ones we received. Exiting the flow....")
        return {"Status": "Failure"}

    img_send_result = send_location_img(image, caption, bot_token, channel_id)

    if img_send_result["Status"] != "Success":
        logging.error("Bot was unable to send the image. Exiting the flow....")
        return

    hh, mm, ss = wait_time.split("-")
    time.sleep(int(hh) * 3600 + int(mm) * 60 + int(ss))

    send_poll_resp = send_poll(bot_token, channel_id, location_ids, poll_q, notion_token, poll_results_dbid, poll_to_event_dbid)

    if send_poll_resp["Status"] != "Success":
        logging.error("Unable to send the poll, Exiting the flow.")
        return
    
    message_id = send_poll_resp["Response"]["result"]["message_id"]
    d, h, m = poll_duration.split("-")
    poll_end_date = today + timedelta(days=int(d), hours=int(h), minutes=int(m))

    logging.info("Project Dir - {}, POll Stop Code - {}".format(cwd, code_loc))

    # Schedule to end the poll
    sch_resp = os.system('echo "{} {} {}" | at {}'.format(venv, os.path.join(cwd, code_loc), message_id, poll_end_date.strftime("%I:%M %p %d.%m.%Y")))
    logging.info("Scheduling response - {}".format(sch_resp))
    

if __name__=="__main__":
    main()
