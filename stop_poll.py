#!/bin/sh

import os
import sys
import requests
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

load_dotenv()
cwd = os.getenv("PROJECT_DIR")

logging.basicConfig(filename='{}/app.log'.format(cwd), format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

def stop_poll(bot_token, channel_id, msg_id):
    url = "https://api.telegram.org/bot{}/stopPoll".format(bot_token)

    data = {
        "chat_id": channel_id,
        "message_id": msg_id,
        "reply_markup": {}
    }
    try:
        response = requests.post(url, json=data)
        logging.info("Response from stop poll api call - {}".format(response.json()))
    except Exception as e:
        logging.error("Unable to stop the poll. Error - {}".format(e))
        return {"Status": "Failure"}
    return {"Status": "Success"}

def main(message_id):
    bot_token, channel_id = os.getenv("API_KEY"), os.getenv("CHANNEL_ID")

    logging.info("Stopping the poll with message id - {}".format(message_id))
    stop_poll(bot_token, channel_id, message_id)

if __name__=="__main__":
    main(sys.argv[1])