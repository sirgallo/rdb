# Load Test


## Locust

Locust is an open source tool written in python for load testing apis


## Test

This test handles mixed load, with
```
90% read
10% write
```


## Run

start master:
```
locust -f locustfile.py --master
```

start workers:
```
locust -f locustfile.py --worker
```