# pinba-server
Alternative server for Pinba (https://github.com/tony2001/pinba_extension)

# How to run
```
# Collect raw pinba packets and "buffer" for 1 sec
./collector --in=0.0.0.0:30002 # pinba should write to this port \
  --out=127.0.0.1:5003
  
# Decode protobuf packets
./decoder --in=127.0.0.1:5003 # collector's --out \
  --out=tcp://127.0.0.1:5005 # it's ZeroMQ PUB Socket

# For test, if we don't want to write to OpenTSDB
nc -l -p 4242 

# "buffer" and aggregate metrics for 10 sec (make it adjustable?) and write metrics to OpenTSDB telnet interface
./aggregator --in=tcp://127.0.0.1:5005 # decoder's --out\
  --out=127.0.0.1:4242
```
