# This is the regex for finding if a given message is an LND invoice to pay.
# This looks for #v4vapp v4vapp
# LND_INVOICE_TAG = r"(.*)(#(v4vapp))"
# Updated to separate the hive name at the start of the message
LND_INVOICE_TAG = r"^\s*(\S+).*#v4vapp"

# magisats_tag should search for #MAGISATS followed by #v4vapp anywhere in the memo no capture
# #MAGISATS needs to be lower case in the regex.
MAGISATS_TAG = r"^\s*\S+.*#magisats(?:\s+(\d+))?.*#v4vapp"
