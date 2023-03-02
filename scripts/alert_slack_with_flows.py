import subprocess
from collections import defaultdict

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

client = WebClient(token="TOKEN")

output = subprocess.check_output(["./active-flow-emails.sh"], universal_newlines=True)

lines = list(filter(None, output.split('\n')))
users = ""
dct_executors = defaultdict(int)
dct_ids = defaultdict(list)
for line in lines:
    email, num_executors, flow_id = line.split(', ')
    dct_executors[email] += int(num_executors)
    dct_ids[email].append(flow_id)

for email, executor_count in sorted(
    dct_executors.items(), key=lambda kv: kv[1], reverse=True
):
    users += f"- {email} has {executor_count} active executors in the flow IDs {', '.join(dct_ids[email])}\n"

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
        {"type": "section", "fields": [{"type": "mrkdwn", "text": users}]},
    ],
}

try:
    response = client.chat_postMessage(**message)
    print("Message sent: ", response["ts"])
except SlackApiError as e:
    print("Error sending message: ", e)
