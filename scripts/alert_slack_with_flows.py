import os
import subprocess
from collections import defaultdict

import requests
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

client = WebClient(token=os.environ.get('SLACK_API_TOKEN'))
script_output = subprocess.check_output(
    [
        f"{os.environ.get('FILE_PATH')}/scripts/active-flow-emails.sh",
        os.environ.get('HUBBLE_M2M_TOKEN'),
    ],
    universal_newlines=True,
)

lines = list(filter(None, script_output.split('\n')))
rows = ""

dict_executors = defaultdict(int)
dict_flow_ids = defaultdict(list)
dict_user_ids = defaultdict(str)

headers = {"Authorization": f"Bearer {os.environ.get('SLACK_API_TOKEN')}"}
teams = requests.get(
    f"https://slack.com/api/conversations.list?limit=300", headers=headers
).json()["channels"]

dict_teams = defaultdict(str)
for team in teams:
    search_areas = tuple([team['name']] + team['previous_names'])
    dict_teams[search_areas] = team['id']


def get_channel_id_by_email(email):
    team_user = email.split('@')[0]
    if team_user == ' team-now-prod':
        team_user = 'team-now'
    for search_items in dict_teams:
        if team_user in search_items:
            return dict_teams[search_items]
    return ""


def map_user_emails(email):
    if '@jina.ai' in email:
        if '+' in email:
            email_parts = email.split('@')
            user_alias = email_parts[0].split('+')[0]
            return user_alias + '@' + email_parts[1]
    return email


def check_email_type(email):
    if '@jina.ai' in email:
        if email.startswith('team'):
            return 'team'
        else:
            return 'member'
    return 'external'


for line in lines:
    email, num_executors, flow_id = line.split(', ')
    dict_executors[email] += int(num_executors)
    dict_flow_ids[email].append(flow_id)
    email_type = check_email_type(email)
    if email_type == 'member':
        response = requests.get(
            f"https://slack.com/api/users.lookupByEmail?email={map_user_emails(email)}",
            headers=headers,
        ).json()
        if 'user' in response:
            dict_user_ids[email] = response['user']['id']
        else:
            dict_user_ids[email] = ""
    elif email_type == 'team':
        dict_user_ids[email] = get_channel_id_by_email(email)

# This variable is used to revert the latest changes in "rows"
rows_mem = ""
for email, executor_count in sorted(
    dict_executors.items(), key=lambda kv: kv[1], reverse=True
):
    rows_mem = rows
    user_tag = f'- <@{dict_user_ids[email]}> | ' if dict_user_ids[email] != '' else '- '
    row = (
        user_tag
        + f"{email} | {executor_count} executors | {len(dict_flow_ids[email])} flows | {', '.join(dict_flow_ids[email])}\n"
    )
    rows += row
    if len(rows) > 2001:
        rows = rows_mem
        break


message = {
    "channel": "#team-search",
    "blocks": [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Hello* 👋 below is the list of users with most active executors, please make sure to clean them up!",
            },
        },
        {"type": "divider"},
        {"type": "section", "fields": [{"type": "mrkdwn", "text": rows}]},
    ],
}

try:
    response = client.chat_postMessage(**message)
    print("Message sent: ", response["ts"])
except SlackApiError as e:
    print("Error sending message: ", e)
