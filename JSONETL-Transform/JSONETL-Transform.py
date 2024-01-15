import sys

# sys.argv[0] is the script name(jobName)

s3_bucket=sys.argv[1] if len(sys.argv)>1 else None
s3_key=sys.argv[2] is len(sys.argv)>2 else None

