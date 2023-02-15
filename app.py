import os
import logging
import requests
import ipaddress
import pandas as pd
from flask import Flask, request
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()
cwd = os.getenv("PROJECT_DIR")

logging.basicConfig(filename='{}/app.log'.format(cwd), format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

app = Flask(__name__)

bot_token = os.getenv("API_KEY")
chat_id = os.getenv("CHANNEL_ID")
poll_result_dbid = os.getenv("POLL_RESULT_DB_ID")
poll_det_result_dbid = os.getenv("POLL_DET_RESULT_DB_ID")

notion_headers = {
    "Authorization": "Bearer " + os.getenv("NOTION_TOKEN"),
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}


telegram_ip_ranges = ['149.154.160.0/20', '91.108.4.0/22']

def is_telegram_request(remote_addr):
    logging.info(remote_addr)
    try:
        for ip_range in telegram_ip_ranges:
            if ipaddress.ip_address(remote_addr) in ipaddress.ip_network(ip_range):
                return True
    except Exception as e:
        logging.error("Unable to validate the source of the request. Error - {}".format(e))
        return False
    return False


@app.route('/', methods=['POST'])
def handle_update():
    logging.info("Request from the source - {} has came to handle update method. Checking if the request came from telegram....".format(request.remote_addr or request.headers.get("X-Forwarded-For", "")))
    if request.method == 'POST' and is_telegram_request(request.remote_addr or request.headers.get("X-Forwarded-For", "")):
        try:
            data = request.get_json()
            logging.info("Received the update - {}. Checking if the data contains results...".format(data))

            # If update is for result count
            if "poll" in data and "total_voter_count" in data["poll"]:
                logging.info("Calling method to update latest results...")
                update_poll_results(data)

            elif "poll_answer" in data and "option_ids" in data["poll_answer"]:
                if len(data["poll_answer"]["option_ids"]) > 0:
                    logging.info("Calling method to insert the user's answer")
                    insert_user_vote(data)
                else:
                    logging.info("User retracted his vote. Calling method to delete the vote.")
                    remove_user_vote(data)

            return 'ok'
        except Exception as e:
            logging.error("Unable to process the incoming request. Error - {}".format(e))
            return 'Not Ok'
    else:
        logging.info("Invalid request method or source.")
        return 'Invalid request', 400


def get_page_id(poll_result):
    url = "https://api.notion.com/v1/databases/{}/query".format(poll_result_dbid)
    
    poll_id = poll_result["poll"]["id"]
    data = {
        "filter": {
            "property": "Poll ID",
            "title": {
                "contains": poll_id
            }
        }
    }

    try:
        logging.info("Making the api call to find the entry that corresponds to the poll - {}".format(poll_id))
        resp = requests.post(url, json=data, headers=notion_headers)
        resp_json = resp.json()
        logging.info("Response from query database to get required page - {}".format(resp_json))
        
        if resp.status_code == 200:
            if len(resp_json["results"]) > 1:
                logging.error("Duplicate entries found in poll result db for poll id - {}.".format(poll_id))
                return {"Status": "Failure"}
            elif len(resp_json["results"]) == 0:
                logging.error("No entry found for the required poll - {}".format(poll_id))
                return {"Status": "Failure"}
            else:
                page_id = resp_json["results"][0]["id"]
    except Exception as e:
        logging.error("Unable to process the poll result to get correspdoning notion page ID. Error - {}".format(e))
        return {"Status": "Failure"}

    return {"Status": "Success", "page_id": page_id}


def update_poll_results(poll_result):
    resp = get_page_id(poll_result)

    if resp["Status"] != "Success":
        logging.error("Not able to find the corresponding entry for poll. Exiting....")
        return "Not Ok"
    try:
        page_id = resp["page_id"]
        url = "https://api.notion.com/v1/pages/{}".format(page_id)

        location_1_votes = poll_result["poll"]["options"][0]["voter_count"]
        location_2_votes = poll_result["poll"]["options"][1]["voter_count"]
        location_3_votes = poll_result["poll"]["options"][2]["voter_count"]
        status = "Closed" if poll_result["poll"]["is_closed"] else "Open"

        data = {
            "Kayo Event 1": {"number": location_1_votes},
            "Kayo Event 2": {"number": location_2_votes},
            "Kayo Event 3": {"number": location_3_votes},
            "Status": {"select": {"name": status}}
        }

        payload = {"properties": data}
        resp = requests.patch(url, json=payload, headers=notion_headers)
        logging.info("Response from update entry api call - {}".format(resp.json()))
    except Exception as e:
        logging.error("Unable to update poll results in db. Error - {}".format(e))

    return resp

def insert_user_vote(poll_data):
    url = "https://api.notion.com/v1/pages"

    poll_id = poll_data["poll_answer"]["poll_id"]
    poll_date = datetime.now().astimezone(timezone.utc).today().date().isoformat()
    userid = poll_data["poll_answer"]["user"]["id"]
    username = poll_data["poll_answer"]["user"].get("username", "")
    first_name = poll_data["poll_answer"]["user"].get("first_name", "")
    last_name = poll_data["poll_answer"]["user"].get("last_name", "")
    option_id_choice = poll_data["poll_answer"]["option_ids"][0]

    event_name = os.getenv("EVENT_NAME")
    poll_to_event_df = pd.read_csv(os.path.join(cwd, "poll_to_events.csv"), dtype=str).set_index("poll_id")
    user_selection =  poll_to_event_df.loc[poll_id, "{}{}".format(event_name, option_id_choice + 1)]

    data = {
        "Poll ID": {"title": [{"text": {"content": poll_id}}]},
        "Date": {"date": {"start": poll_date, "end": None}},
        "UserID": {"number": userid},
        "Username": {"rich_text": [{"text": {"content": username}}]},
        "First Name": {"rich_text": [{"text": {"content": first_name}}]},
        "Last Name": {"rich_text": [{"text": {"content": last_name}}]},
        "Choice": {"rich_text": [{"text": {"content": user_selection}}]},
    }

    payload = {
        "parent": {
            "database_id": poll_det_result_dbid
            },
            "properties": data
    }

    try:
        response = requests.post(url, json=payload, headers=notion_headers)
        logging.info("Response from insert user vote api call - {}".format(response.json()))
    except Exception as e:
        logging.error("Unable to insert the user vote in detailed poll result DB. Error - {}".format(e))
        {"Status": "Failure"}
    return {"Status": "Success"}


def get_user_page_id(poll_result):
    url = "https://api.notion.com/v1/databases/{}/query".format(poll_det_result_dbid)
    
    user_id = poll_result["poll_answer"]["user"]["id"]
    poll_id = poll_result["poll_answer"]["poll_id"]

    data = {
        "filter":{
        "and" : [{
            "property": "UserID",
            "number": {
                "equals": user_id
            }
        },
        {
            "property": "Poll ID",
            "title": {
                "contains": poll_id
            }
        }]}
    }

    try:
        logging.info("Making the api call to find the entry that correspondsing to the user - {} vote for poll - {}".format(user_id, poll_id))
        resp = requests.post(url, json=data, headers=notion_headers)
        resp_json = resp.json()
        logging.info("Response from query database to get required page - {}".format(resp_json))
        
        if resp.status_code == 200:
            if len(resp_json["results"]) > 1:
                logging.error("Duplicate entries found in poll result db for poll id - {}.".format(poll_id))
                return {"Status": "Failure"}
            elif len(resp_json["results"]) == 0:
                logging.error("No entry found for the required poll - {}".format(poll_id))
                return {"Status": "Failure"}
            else:
                page_id = resp_json["results"][0]["id"]
        else:
            return {"Status": "Failure"}
    except Exception as e:
        logging.error("Unable to find the page id of the user entry in db for user - {}. Error - {}".format(user_id, e))
        return {"Status": "Failure"}

    return {"Status": "Success", "page_id": page_id}

def remove_user_vote(poll_user_data):
    resp = get_user_page_id(poll_user_data)

    if resp["Status"] != "Success":
        logging.error("Not able to find the corresponding entry for user vote for the poll. Exiting....")
        return "Not Ok"

    page_id = resp["page_id"]
    url = "https://api.notion.com/v1/pages/{}".format(page_id)
    payload = {"archived": True}

    try:
        resp = requests.patch(url, json=payload, headers=notion_headers)
        logging.info("Response from remove user entry api call since user has retracted the vote - {}".format(resp.json()))
    except Exception as e:
        logging.error("Unable to remove user vote entry. Error - {}".format(e))
        return "Not Ok"
    return "Ok"

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0')