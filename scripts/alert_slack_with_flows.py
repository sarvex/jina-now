import os
import subprocess
from collections import defaultdict

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

print('SCRIPT OUT: ', script_output)
lines = list(filter(None, script_output.split('\n')))
rows = ""

dict_executors = defaultdict(int)
dict_ids = defaultdict(list)
for line in lines:
    email, num_executors, flow_id = line.split(', ')
    dict_executors[email] += int(num_executors)
    dict_ids[email].append(flow_id)

# This variable is used to revert the latest changes in "rows"
rows_mem = ""
for email, executor_count in sorted(
    dict_executors.items(), key=lambda kv: kv[1], reverse=True
):
    rows_mem = rows
    rows += f"- {email} | {executor_count} executors | flow IDs {', '.join(dict_ids[email])}\n"
    if len(rows) > 2001:
        rows = rows_mem
        break


message = {
    "channel": "#slack-function-test",
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
