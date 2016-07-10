# DistributedSystems

# GroupMessenger2

* **Description** - The application would multicast every user-entered message to all app instances (including the one that is sending the message). The app would use B-multicast and ensures FIFO and Total ordering. Atmost one device would fail during the communication and the app still ensures FIFO and Total ordering under such situation. 

* **Storage** - Every message would be stored in the provider individually by all app instances. Each message would be stored as a <key, value> pair. The key would be the final delivery sequence number for the message (as a string); the value would be the actual message (again, as a string). The delivery sequence number would start from 0 and increase by 1 for each message.

* **Server and Ports** - The app would open one server socket that listens on 10000. 5 AVDs are used to simulate a distributed system setting. The redirection ports are 11108, 11112, 11116, 11120, and 11124.  

* **Implementation** - We should implement a decentralized algorithm to handle failures correctly. This means that we should not implement a centralized algorithm. This also means that we should not implement any variation of a centralized algorithm that randomly picks a central node as this result in killing the AVD which was selected as the sequencer. [*ISIS Algorithm*](https://courses.engr.illinois.edu/cs425/fa2013/L7.fa13.ppt) has been chosen to meet the criteria of the requirements.

# Simple Distributed Hash Table(Dht)

* **Server and Ports** - The app would open one server socket that listens on 10000. 5 AVDs are used to simulate a distributed system setting. The redirection ports are 11108, 11112, 11116, 11120, and 11124.  

* **Features** - ID space partioning, Ring-based routing and Node joins. Keys were generated using SHA-1 hash.

# Simple Dynamo

* **Assumptions** 
  * Atmost one node failure at a give time
  * Failures are temporary
  * Every Node can know every other node
  
* **Server and Ports** - The app would open one server socket that listens on 10000. 5 AVDs are used to simulate a distributed system setting. The redirection ports are 11108, 11112, 11116, 11120, and 11124.   
 
* **Features** - Partioning, Replication and Failure Handling using Chain replication. 

## Authors

* [**Steve Ko**](https://nsr.cse.buffalo.edu/?page_id=272) - *Initial author*
