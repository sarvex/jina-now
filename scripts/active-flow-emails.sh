#!/bin/sh


token="$1"
kubectl get pods -A | grep jnamespace | awk '{print $1}' | uniq -c | sort -nr | head -n 5 | while read name; do
  echo $name
  num_executors=$(echo "$name" | cut -d' ' -f1)
  namespace=$(echo "$name" | cut -d' ' -f2)
  flow_id=$(echo "$name" | cut -d- -f2-2)
  user_id=`kubectl get flow -n $namespace -o 'jsonpath={.items[*].metadata.labels.jina\.ai\/user}'`
  user_email=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -H "Authorization: $token" \
    -d '{"ids": ["'${user_id}'"]}' "https://api.hubble.jina.ai/v2/rpc/user.m2m.listUserInfo" \
    | jq -r '.data[0].email')
    echo "$user_email, $num_executors, $flow_id"
done