# Distributed Key-Value Database

The following repository contains a simple distributed key-value database

## Features

- Load Balancing - Utilizes Consistent Hashing
- Data Replication Mechanism - Data is lazily replicated on two other servers for every write request (Given 3 servers running)
- Graceful Server Failure Recovery - Data is automatically recovered in the case of server crashes
- HTTP Server - Running regex query operations for fetching/inserting/modifying key-value pairs

Refer to the linked design documents in the releases for an in-depth look into the performance and functionality of the database. 

## Client Communication API

``connect <address> <port>`` - Eastablish a TCP connection to the storage server  
``disconnect``- Disconnect from the connected server  
``put <key> <value>`` :   
  - **Insert** key-value pair into the storage system
  - **Update** (overwrite) key-value pair into the storage system if the given key already exists
  - **Delete** key-value pair if the ``<value>`` is null  

``get <key>`` - Retrieves given key from storage system  
``logLevel <level>`` - Sets log level for insepcting commands  
``help``- Displays all commands available  
``quit`` - Tear down active connection and exit client program

## Single Server Response

| Request                |  Result                     | Response                       |
|------------------------|-----------------------------|--------------------------------|
| ``put <key> <value>``  | ``Tuple Inserted``          | ``PUT_SUCCESS<key,value>``     |
| ``put <key> <value>``  | ``Tuple Updated``           | ``PUT_UPDATE<key,value>``      |
| ``put <key> <value>``  | ``Unable to Insert Tuple``  | ``PUT_ERROR<key,value>``       |
| ``put <key> <null>``   | ``Deleted Tuple``           | ``DELETE_SUCCESS<key,value>``  |
| ``put <key> <null>``   | ``Unable to Delete Tuple``  | ``DELETE_ERROR<key,value>``    |
| ``get <key>``          | ``Tuple Found``             | ``GET_SUCCESS<key,value>``     |
| ``get <key>``          | ``Tuple not Found``         | ``GET_ERROR<key,value>``       |

## Server Controller API - ECS 

Responsible for adding/removing/stopping/starting servers in your distributed database

- ``addNodes <numberOfNodes>`` - Initializes the specified number of servers inserting them into a queue to be started
- ``start`` - Starts all initialized servers within the queue
- ``stop`` - Stops all servers currently running
- ``shutdown`` - Shutdown all servers currently running
- ``removeNode`` - removes a random server that is currently running

## HTTP Server API (Executing Regex Query Expressions)

Find details on the the Regex API specifications [here](https://petstore.swagger.io/?url=https://raw.githubusercontent.com/ArjunM98/DistributedSystems/blob/main/src/app_kvHttp/resources/openapi.yaml)

## Build/Run Instructions

### Pre-requisites 
- Java
- Ant
- Apache Zookeeper

### Building the application

``ant`` - Builds the project generating 4 jar files
- ``m4-client.jar``
- ``m4-ecs.jar``
- ``m4-http.jar``
- ``m4-server.jar``

### Running the Client interface

``java -jar m4-client.jar``  - Starts the terminal client interface

### Running the ECS Contoller

1. Before starting ECS, you must ensure an instance of apache zookeeper is running. This can be done by navigating to the apache-zookeeper folder and running ``bin/zkServer.sh start``   
- You can stop apache-zookeeper by running the corresponding command: ``bin/zkServer.sh stop``
2. Create a file called ``ecs.config`` which contains a list of: ``<server_name> <host> <port>``
3. ``java -jar m4-ecs.jar ecs.config <apache-zookeeper port>`` - Note there is a default value provided for ``<apache-zookeeper port>`` and does not need to be provided

### Running the HTTP Server

``java -jar m4-server.jar <host> <port> ``  - Starts the HTTP Server at the specified ``<host> <port>``
