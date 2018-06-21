# ReduceMap
MapReduce in Java

## Introduction
MapReduce in Java, with basic master and worker implementation.

[Paper](http://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf)
## Project Structure
### RPC Framework
Use [grpc](http://grpc.io) as RPC framework.

protobuf3 files:
- master.proto: define master RPC methods, called by workers.
- worker.proto: define worker RPC methods, called by master.

protobuf generated code:
in target/generated-sources/ **(Not uploaded)**
### Master
- Master.java: the ReduceMap master implementation.
- MasterRPCClient.java: used to call RPC methods.
- MasterRPCServer.java: used to receive RPC requests.
- WorkerManager.java: manage workers' status.
### Worker
- Worker.java: the ReduceMap worker implementation.
- WorkerRPCClient.java: used to call RPC methods.
- WorkerRPCServer.java: used to receive RPC requests.
### Common
- RPCAddress.java: represents host address(ip & port).
- RPCConfig.java: basic RPC Configuration.

## Build
The project uses Maven to manage build.

The configuration file is pom.xml **(do not modify)**

- using IntelliJ IDEA (any platform) (recommended)
    1. import the project as a Maven project.
    2. switch to pom.xml and update Maven project.
- use command line (Linux maybe)
    1. install maven.
    2. Use maven to build project.

## Usage
### Master
Master.main [port]

port: port for master to run on (default: 50051) 
### Worker
Worker.main [port] master_address master_port

port: port for worker to run on (default: 50052)

master_address: IP address of master (default: localhost)

master_port: port of master (default: 50051) 

## Implementaion & Dev
### Master
Supported Master service(called by worker):
- onRegister: register new worker and assign id
- onHeartbeat: keep alive worker

Supported Worker service(called by master):
- AssignWork: assign work to worker (demo for now, needs rewrite)
- Halt: stop worker (when no work to assign, etc.)

Function:
- Maintain worker info(address, id, liveness, etc.)
- Schedule tasks for workers
- Merge worker outputs
- Handle worker failures
### Worker
Supported Worker service(called by master):
- onAssignWork: handle AssignWork request from master
- onHalt: handle Halt request from master

Supported Master service(called by worker):
- Register: send address to master and obtain an id
- Heartbeat: send liveness information to master

Function:
- Receive task
- Read in data
- Process data(map or reduce or anything)
- Output result
### RPC methods
- to call master(in worker):
```java
client.<method_name>(<args>);
```
- to call worker(in master):
```java
int id = <worker_id>;
workerMan.getWorkerRPC(id).<method_name>(<args>);
```
### TODOs:
- make presentation
- master implementation
- worker implementation
- design map & reduce function
- testing
- demo

## Test & Result

### Test environment
1. local test

    run master and all workers on localhost