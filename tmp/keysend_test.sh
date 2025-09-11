# Umbrel
dest_pubkey="0396693dee59afd67f178af392990d907d3a9679fa7ce00e806b8e373ff6b70bd8"
name="Umbrel"
echo "********************************"
currency="HIVE"
amount="1"
notify="false"
echo "Sending "$amount" "$currency" to "$name" - Notify: "$notify

curl -X 'POST' \
  'http://adam-v4vapp:8000/keysend/send?notify='$notify'&amt='$amount'&currency='$currency'&dest_pubkey='$dest_pubkey'&add_text=0' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "action": "boost",
  "app_name": "V4Vapp",
  "podcast" : "Podcasting 2.0",
  "sender_name": "Brianoflondon and Hive DAO",
  "name" : "'$name'",
  "message" : "Bit of a catch up donation... though my main Hive DAO funding has reduced quite a bit but as Freddie sang: the show must go on!"
}' | python3 -m json.tool

echo "********************************"
