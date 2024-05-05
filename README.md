# Cloud-based Search Engine
### Kevin Zhang, Spencer Mateega, Manvi Kaul, and Mahika Vajpeyi

## Run Instructions

The project components are split into 4 parts: a crawler, an indexer, PageRank, and a front end.

### Running the crawler on one instance

Boot up one instance on EC2, clone this repo, and cd into the Crawler folder.

First run the following command in the instance’s terminal to compile the code:
```sh
javac -cp lib/webserver.jar:lib/kvs.jar:lib/flame.jar:lib/json-20140107.jar --source-path src -d bin $(find src -name '*.java')
```
Then, open up 10 new terminals. Run each of the following lines in seperate terminals:

```sh
nohup java -cp bin:lib/webserver.jar:lib/kvs.jar cis5550.kvs.Coordinator 8000
nohup java -cp bin:lib/webserver.jar:lib/kvs.jar cis5550.kvs.Worker 8001 worker1 localhost:8000
nohup java -cp bin:lib/webserver.jar:lib/kvs.jar cis5550.kvs.Worker 8002 worker2 localhost:8000
nohup java -cp bin:lib/webserver.jar:lib/kvs.jar cis5550.kvs.Worker 8003 worker3 localhost:8000
nohup java -cp bin:lib/webserver.jar:lib/kvs.jar cis5550.kvs.Worker 8004 worker4 localhost:8000
nohup java -cp bin:lib/webserver.jar:lib/kvs.jar:lib/flame.jar cis5550.flame.Coordinator 9000 localhost:8000
nohup java -cp bin:lib/webserver.jar:lib/kvs.jar:lib/flame.jar cis5550.flame.Worker 9001 localhost:9000
nohup java -cp bin:lib/webserver.jar:lib/kvs.jar:lib/flame.jar cis5550.flame.Worker 9002 localhost:9000
nohup java -cp bin:lib/webserver.jar:lib/kvs.jar:lib/flame.jar cis5550.flame.Worker 9003 localhost:9000
nohup java -cp bin:lib/webserver.jar:lib/kvs.jar:lib/flame.jar cis5550.flame.Worker 9004 localhost:9000
```

This will start a KVS coordinator, 4 KVS workers, a Flame coordinator, and 4 Flame workers. Go to the 8000 port on the instance’s public IP to view the KVS coordinator and port 9000 to view the Flame coordinator.


### Running the front end

Assuming the instance is booted up and PageRank has finished running, close down the coordinators and workers (using Comand C or  `ps -ef |grep java` and `kill [PID]`) and recompile the code using the following command:

```sh
javac -cp lib/webserver.jar:lib/kvs.jar:lib/flame.jar:lib/json-20140107.jar --source-path src -d bin $(find src -name '*.java')
```

Then reboot the coordinators and workers by opening 10 new terminals and run each of the following lines in a separate terminal:
 
```sh
nohup java -cp bin:lib/webserver.jar:lib/kvs.jar cis5550.kvs.Coordinator 8000
nohup java -cp bin:lib/webserver.jar:lib/kvs.jar cis5550.kvs.Worker 8001 worker1 localhost:8000
nohup java -cp bin:lib/webserver.jar:lib/kvs.jar cis5550.kvs.Worker 8002 worker2 localhost:8000
nohup java -cp bin:lib/webserver.jar:lib/kvs.jar cis5550.kvs.Worker 8003 worker3 localhost:8000
nohup java -cp bin:lib/webserver.jar:lib/kvs.jar cis5550.kvs.Worker 8004 worker4 localhost:8000
nohup java -cp bin:lib/webserver.jar:lib/kvs.jar:lib/flame.jar cis5550.flame.Coordinator 9000 localhost:8000
nohup java -cp bin:lib/webserver.jar:lib/kvs.jar:lib/flame.jar cis5550.flame.Worker 9001 localhost:9000
nohup java -cp bin:lib/webserver.jar:lib/kvs.jar:lib/flame.jar cis5550.flame.Worker 9002 localhost:9000
nohup java -cp bin:lib/webserver.jar:lib/kvs.jar:lib/flame.jar cis5550.flame.Worker 9003 localhost:9000
nohup java -cp bin:lib/webserver.jar:lib/kvs.jar:lib/flame.jar cis5550.flame.Worker 9004 localhost:9000
```

Now head over to the index.html page in a browser (http://[INSTANCE PUBLIC IP].index.html) to view the completed search engine!
 
 ## External Resources
The only external package we used was a JAR to import JSON objects and JSON arrays, which can be downloaded from [Maven](https://mvnrepository.com/artifact/org.json/json/20140107). 
 
## Instance Types
Throughout the project, c5ad.2xlarge instances were used.
 

