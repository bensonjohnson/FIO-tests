import sys
import os
import json
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# InfluxDB 2 connection details
url = "http://10.0.0.85:8086"
token = "OcQvweCK7nP23ZeTrMmhje4AVF6vD9CfL-mhBim0heQa81qe3vNdhU7deqkguE6WglRhauMGCUTg-JLFDy0j5w=="
org_id = "posix"
bucket = "fio_test"

# Connect to InfluxDB
client = InfluxDBClient(url=url, token=token, org=org_id)

# Create a write API instance
write_api = client.write_api(write_options=SYNCHRONOUS)

# Test the InfluxDB connection
try:
    ping = client.ready()
    if ping:
        print("InfluxDB connection is healthy")
    else:
        print("InfluxDB connection is not healthy")
except Exception as e:
    print("InfluxDB connection test failed:", str(e))
    sys.exit(1)

# Read JSON data from stdin
fio_data = json.load(sys.stdin)

# Extract job name
job_name = sys.argv[1]

timestamp = int(os.stat(sys.argv[2]).st_mtime) * 1000000000  # InfluxDB expects nanoseconds

# Log job-level data
for key, value in fio_data.items():
    if key != "jobname" and not isinstance(value, dict):
        point = Point("fio").tag("job", job_name).field(key, value).time(timestamp, WritePrecision.NS)
        write_api.write(bucket=bucket, record=point)

# Log job options data (converted to string)
job_options = fio_data.get("job options")
if job_options:
    job_options_str = json.dumps(job_options)
    point = Point("fio").tag("job", job_name).field("job_options", job_options_str).time(timestamp, WritePrecision.NS)
    write_api.write(bucket=bucket, record=point)

# Log group-level data
for group in fio_data.get("groups", []):
    group_id = group.get("groupid", 0)
    for key, value in group.items():
        if key != "groupid" and not isinstance(value, dict):
            point = (
                Point("fio")
                .tag("job", job_name)
                .tag("group", group_id)
                .field(key, value)
                .time(timestamp, WritePrecision.NS)
            )
            write_api.write(bucket=bucket, record=point)

# Log thread-level data
for group in fio_data.get("groups", []):
    group_id = group.get("groupid", 0)
    for thread in group.get("threads", []):
        thread_id = thread.get("threadid", 0)
        for key, value in thread.items():
            if key != "threadid" and not isinstance(value, dict):
                point = (
                    Point("fio")
                    .tag("job", job_name)
                    .tag("group", group_id)
                    .tag("thread", thread_id)
                    .field(key, value)
                    .time(timestamp, WritePrecision.NS)
                )
                write_api.write(bucket=bucket, record=point)

# Close the InfluxDB client
client.close()