kubectl get pods -A | grep jnamespace | awk '{print $1}' | uniq -c | sort -nr | while read name; do
  num_flows=$(echo "$name" | cut -d' ' -f1)
  namespace=$(echo "$name" | cut -d' ' -f2)
  flow_id=$(echo "$name" | cut -d- -f2-2)
  echo $namespace
  user_id=`kubectl get jcloud -n $namespace -o 'jsonpath={.items[*].metadata.labels.jina\.ai\/user}'`
  user_email=$(curl -X POST \
    -H "Content-Type: application/json" \
    -H "Authorization: Basic MjdiOTQ4MWI0MTEyYWU2OWYyY2MxMGEyM2Q2YTNkY2I6NmYzMGMwOGJkNTc4OTQ2ZGFlZTY1NDY2YmNjNDM0YzNmZDY4OTIxODhlYmFiYmU4ZmM5NzIxOGMzMDYyNTQ1NQ==" \
    -d '{"ids": ["'${user_id}'"]}' "https://api.hubble.jina.ai/v2/rpc/user.m2m.listUserInfo" \
    | jq -r '.data[0].email')

    echo "$user_email has $num_flows active executors in the flow ID $flow_id"
done