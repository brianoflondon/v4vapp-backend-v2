import json
import shutil
import os

input_file = '/Users/bol/Documents/dev/v4vapp/v4vapp-backend-v2/logs_docker/voltage/v4vapp-backend-v2.log.jsonl'
backup_file = '/Users/bol/Documents/dev/v4vapp/v4vapp-backend-v2/logs_docker/voltage/v4vapp-backend-v2.log.backup.jsonl'
output_file = '/Users/bol/Documents/dev/v4vapp/v4vapp-backend-v2/logs_docker/voltage/v4vapp-backend-v2.unprettified.log.jsonl'

# Step 1: Copy the log file to a backup
shutil.copy(input_file, backup_file)
print(f"Backup of log file created at {backup_file}")

# Step 2: Un-prettify the JSON log file
with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
    buffer = ""
    for line in infile:
        buffer += line.strip()
        if buffer.endswith('}'):
            try:
                json_obj = json.loads(buffer)
                compact_json = json.dumps(json_obj, separators=(',', ':'))
                outfile.write(compact_json + '\n')
                buffer = ""
            except json.JSONDecodeError:
                # If JSON is not complete, continue accumulating lines
                continue

print(f"Un-prettified JSON log file written to {output_file}")

# Step 3: Overwrite the original log file with the un-prettified log file
shutil.move(output_file, input_file)
print(f"Original log file overwritten with un-prettified log file")