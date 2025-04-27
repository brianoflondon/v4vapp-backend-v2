# import base64
# import json


# def b64_decode(base64_message):
#     """Just decode the base64 message"""
#     if base64_message is None:
#         return ""

#     base64_bytes = base64_message.encode("utf-8")
#     message_bytes = base64.b64decode(base64_bytes)
#     message = message_bytes.decode("utf-8")
#     if is_json(message):
#         return json.loads(message)
#     else:
#         return message

# def is_json(myjson):
#     try:
#         json.loads(myjson)
#     except ValueError:
#         return False
#     return True


# def decode_custom_records(custom_records: dict) -> dict:
#     """Explict code paths for different apps"""
#     ans = {}
#     key_set = set(custom_records.keys())

#     b64decode_set = {"7629169", "7629171", "7629173", "7629175"}

#     breez_set = {"7629171", "7629169", "5482373484"}
#     sphinx_set = {"133773310", "5482373484"}

#     hive_accname = None
#     # First check for the Hive Record
#     if value := custom_records.get("818818"):
#         hive_accname = b64_decode(value)
#         hive_accname = check_hive_accname(hive_accname)
#         # hive_accname = ""
#         custom_records.pop("818818")
#         # decoded.append({"hive_accname":hive_accname})

#     if key_set == breez_set:
#         b64_mes = custom_records.get("7629169")
#         ans = b64_decode(b64_mes)
#         b64_mes = custom_records.get("7629171")
#         ans["value_recipient"] = b64_decode(b64_mes)
#         ans["app_name"] = "breez"
#         if hive_accname:
#             ans["hive_accname"] = hive_accname
#         return ans

#     if key_set == sphinx_set:
#         b64_mes = custom_records.get("133773310")
#         m = b64_decode(b64_mes)
#         blob = re.search(r"(^{.*})", m)
#         if blob:
#             ans["133773310"] = json.loads(blob[0])
#             content = ans["133773310"].get("message").get("content")
#             if content:
#                 c = json.loads(content)
#                 # Throw away most of what Sphinx sends
#                 # ans["133773310"]["message"]["content"] = c
#                 ans = c
#         ans["app_name"] = "sphinx"
#         # Sphinx doesn't have boost only stream
#         ans["action"] = "stream"
#         if hive_accname:
#             ans["hive_accname"] = hive_accname
#         return ans

#     decoded = []
#     for record in custom_records.items():
#         if record[0] in b64decode_set:
#             decoded.append(b64_decode(record[1]))

#     if hive_accname:
#         decoded.append({"hive_accname": hive_accname})

#     ans = {}
#     # Special case for Breez
#     # if decoded[0].get("app_name") == "Breez":
#     #     decoded[1] = {"name":decoded[1]}
#     for dec in decoded:
#         if type(dec) == dict:
#             ans.update(dec)

#     # Fix Podverse problem
#     if ans.get("action") == "streaming":
#         ans["action"] = "stream"

#     return ans


# class KeysendCustomRecord(BaseModel):
#     podcast: Optional[str] = Field(None, description="Title of the podcast")
#     feedID: Optional[int] = Field(None, description="ID of podcast in PodcastIndex.org directory")
#     url: Optional[AnyUrl] = Field(None, description="RSS feed URL of podcast")
#     guid: Optional[UUID] = Field(
#         None,
#         description="The `<podcast:guid>` tag. See https://github.com/Podcastindex-org/podcast-namespace/blob/main/docs/1.0.md#guid",
#     )
#     #
#     episode: Optional[str] = Field(None, description="Title of the podcast episode")
#     itemID: Optional[int] = Field(None, description="ID of episode in PodcastIndex.org directory")
#     episode_guid: Optional[str] = Field(None, description="The GUID of the episode")
#     #
#     time: Optional[str]
#     ts: Optional[int]
#     action: Optional[str] = "stream"
#     app_name: Optional[str] = "unknown"
#     app_version: Optional[str]
#     boost_link: Optional[str]
#     message: Optional[str]
#     name: Optional[str]
#     pubkey: Optional[str]
#     sender_key: Optional[str]
#     sender_name: Optional[str]
#     sender_id: Optional[str]
#     sig_fields: Optional[str]
#     signature: Optional[str]
#     speed: Optional[str]
#     boost_uuid: Optional[str]
#     stream_uuid: Optional[str]
#     uuid: Optional[str]
#     value_msat: Optional[int]
#     cr_value_msat: Optional[int]
#     value_msat_total: Optional[int]
#     reply_address: Optional[str]
#     reply_custom_key: Optional[str]
#     reply_custom_value: Optional[str]
#     remote_feed_guid: Optional[str]
#     remote_item_guid: Optional[str]

#     class Config:
#         json_encoders = {UUID: lambda v: str(v)}  # Convert UUID to string

#     @property
#     def trx_reason(self) -> TrxReason:
#         """
#         Returns the trx reason for keysend invoice with this action
#         This is where we will switch to KEEPSATS_BOOST and KEEPSATS_STREAMING
#         """
#         if self.action == "boost":
#             return TrxReason.KEEPSATS_BOOST
#         elif self.action == "auto":
#             return TrxReason.KEEPSATS_AUTO
#         else:
#             return TrxReason.KEEPSATS_STREAMING
#         # if self.action == "boost":
#         #     return TrxReason.BOOST
#         # elif self.action == "auto":
#         #     return TrxReason.AUTO
#         # else:
#         #     return TrxReason.STREAMING

#     @property
#     def action_type(self) -> str:
#         """
#         Returns the action type for keysend invoice with this action
#         Returns "stream" or "boost" for anything that isn't a stream
#         """
#         if self.action == "stream":
#             return "stream"
#         return "boost"

#     def dict_uuid_to_str(self, **kwargs) -> dict:
#         """Returns a dict with all UUIDs converted to strings"""
#         d = self.dict(**kwargs)
#         for k, v in d.items():
#             if isinstance(v, UUID):
#                 d[k] = str(v)
#         return d

#     @property
#     def unique_listen_event_id(self) -> str:
#         if self.action == "boost":
#             if self.boost_uuid:
#                 return self.boost_uuid
#         if self.action == "stream":
#             if self.stream_uuid:
#                 return self.stream_uuid

#         # podcast = f"{self.guid}-{self.podcast}-{self.feedID}-{self.url}"
#         podcast = next(
#             item for item in [str(self.guid), self.podcast, self.feedID, self.url] if item
#         )
#         # episode = f"{self.episode}-{self.itemID}-{self.episode_guid}"
#         episode = next(item for item in [self.episode, self.itemID, self.episode_guid] if item)
#         player = f"{self.app_name}-{self.app_version}"
#         action = f"{self.action}"
#         # replace spaces with underscores
#         unique_id = f"{podcast}-{episode}-{player}-{action}"
#         unique_id = re.sub(r"[ /:]", "_", unique_id)
#         return unique_id

#     def __init__(__pydantic_self__, **data: Any) -> None:
#         """
#         Cludge for Fountain problem 2023-05-15
#         This fixes a problem where Fountain sent a string instead of an int
#         for the itemID in the keysend invoice TLV records. Without this the
#         code will throw multiple input errors every time it re-scans the whole
#         database.
#         """
#         if data.get("itemID") == "cB5cg0whBP9RuEOyH08j":
#             data["itemID"] = 15465533793

#         # Cludge for Errheads bad tests 2023-06-12 added
#         if data.get("app_name") == "PeerTube" and (
#             data.get("app_version") == "4.2.8" or data.get("app_version") == "4.2.6"
#         ):
#             data.pop("guid", None)
#             data.pop("reply_address", None)

#         if data.get("guid") == "":
#             # remove guid item from data if it is empty
#             # Podverse problem 2023-10-27
#             data.pop("guid", None)
#             logging.warning("Empty guid removed from keysend TLV import data")
#             try:
#                 logging.warning(json.dumps(data))
#             except Exception:
#                 pass

#         super().__init__(**data)
