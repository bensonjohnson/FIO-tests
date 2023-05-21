import sys
import os
import re
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

# Read plain text data from stdin
fio_output = sys.stdin.read()

# Regular expression patterns to extract data
jobname_pattern = re.compile(r'Jobname:\s+(\S+)')
job_options_pattern = re.compile(r'(?s)Job options:\s+(.*?)\n\n')
group_data_pattern = re.compile(r'(?s)Group\s+\d+:\s+(.*?)\n\n')
thread_data_pattern = re.compile(r'(?s)Thread\s+\d+:\s+(.*?)\n\n')

# Extract job-level data
jobname_match = jobname_pattern.search(fio_output)
if jobname_match:
    job_name = jobname_match.group(1)
else:
    print("Failed to extract job name from FIO output")
    sys.exit(1)

timestamp = int(os.stat(sys.argv[1]).st_mtime) * 1000000000  # InfluxDB expects nanoseconds

# Extract job options data
job_options_match = job_options_pattern.search(fio_output)
if job_options_match:
    job_options_str = job_options_match.group(1)
    job_options = dict(re.findall(r'(\S+)=(\S+)', job_options_str))
else:
    job_options = {}

# Log job-level data
for key, value in job_options.items():
    point = Point("fio").tag("job", job_name).field(key, value).time(timestamp, WritePrecision.NS)
    write_api.write(bucket=bucket, record=point)

# Log group-level and thread-level data
group_matches = group_data_pattern.findall(fio_output)
for group_match in group_matches:
    group_lines = group_match.strip().split('\n')
    group_id = group_lines[0].split(':')[0].strip()
    group_data = dict(re.findall(r'(\S+)=(\S+)', '\n'.join(group_lines[1:])))
    
    # Log group-level data
    for key, value in group_data.items():
        point = Point("fio").tag("job", job_name).tag("group", group_id).field(key, value).time(timestamp, WritePrecision.NS)
        write_api.write(bucket=bucket, record=point)
    
    # Log thread-level data
    thread_matches = thread_data_pattern.findall(group_match)
    for thread_match in thread_matches:
        thread_lines = thread_match.strip().split('\n')
        thread_id = thread_lines[0].split(':')[0].strip()
        thread_data = dict(re.findall(r'(\S+)=(\S+)', '\n'.join(thread_lines[1:])))
        
        # Log thread-level data
        for key, value in thread_data.items():
            point = Point("fio").tag("job", job_name).tag("group", group_id).tag("thread", thread_id).field(key, value).time(timestamp, WritePrecision.NS)
            write_api.write(bucket=bucket, record=point)

# Close the InfluxDB client
client.close()