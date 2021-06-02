# DistributedSystems

The following repository contains a simple distributed key-value database

## Features

- Load Balancing - Utlizies Consistent Hashing
- Data Replication Mechanism - Data is replicated on 2 other servers for every write request given that there are 3 servers running
- Graceful Server Failure Recovery - Data is automitcally recovered in the case of server crashes
- HTTP Server - Running regex query operations for fetching/inserting/modifying key-value pairs

Refer to the linked design documents in the releases for an indepth look into the performance and funcitonality of the database. 

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

## Server Response API

| Request                |  Result                     | Response                       |
|------------------------|-----------------------------|--------------------------------|
| ``put <key> <value>``  | ``Tuple Inserted``          | ``PUT_SUCCESS<key,value>``     |
| ``put <key> <value>``  | ``Tuple Updated``           | ``PUT_UPDATE<key,value>``      |
| ``put <key> <value>``  | ``Unable to Insert Tuple``  | ``PUT_ERROR<key,value>``       |
| ``put <key> <null>``   | ``Deleted Tuple``           | ``DELETE_SUCCESS<key,value>``  |
| ``put <key> <null>``   | ``Unable to Delete Tuple``  | ``DELETE_ERROR<key,value>``    |
| ``get <key>``          | ``Tuple Found``             | ``GET_SUCCESS<key,value>``     |
| ``get <key>``          | ``Tuple not Found``         | ``GET_ERROR<key,value>``       |
