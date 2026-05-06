# Distributed KV Store

A strongly consistent distributed key-value store using logical partitioning, replication, and the Raft consensus algorithm.


## Features

* **Strong Consistency** via Raft consensus
* **Logical Partitioning** of keyspace using hashing
* **Replication** across nodes for fault tolerance
* **Cluster Setup**: 15 nodes (3 partitions × 5 nodes each)
* **Smart Load Balancer** for request routing
* **RESP-based Protocol** (supports `GET`, `SET`, `DEL`)


## Architecture Overview

Instead of physical shards, the system uses **logical partitions**:

* The keyspace is divided using a hash function
* Each key is mapped to a **partition**
* Each partition is backed by a **Raft cluster (5 nodes)**

```id="ks40zv"
Client → Load Balancer → Hash(key) → Partition → Raft Cluster
```

The load balancer:

* Parses the RESP request
* Extracts the key
* Computes its hash
* Routes the request to the correct partition

## Important Detail

These are **logical partitions**, not independent shard servers:

* Data distribution is based on hashing
* Partitions are pre-defined (no dynamic rebalancing yet)
* Each partition is tightly coupled with its Raft group
* 


## How to Run it 

1. Clone the Repository
2. Start the Backend : 
       cd Backend
       docker compose up (make sure to have docker installed) this will start the nodes and raft consesus between them
3. Start the Frontend:
       cd Frontend
       npm run dev
