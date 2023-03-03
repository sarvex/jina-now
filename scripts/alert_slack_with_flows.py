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

for email, executor_count in sorted(
    dict_executors.items(), key=lambda kv: kv[1], reverse=True
):
    rows += f"- {email} has {executor_count} active executors in the flow IDs {', '.join(dict_ids[email])}\n"

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

print(message)
try:
    response = client.chat_postMessage(**message)
    print("Message sent: ", response["ts"])
except SlackApiError as e:
    print("Error sending message: ", e)
