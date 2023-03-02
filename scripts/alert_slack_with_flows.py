from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

client = WebClient(token="TOKEN")

# output = subprocess.check_output(["./active-flow-emails.sh"], universal_newlines=True)

try:
    response = client.chat_postMessage(channel="#zac-test", text='test')
    print("Message sent: ", response["ts"])
except SlackApiError as e:
    print("Error sending message: ", e)
