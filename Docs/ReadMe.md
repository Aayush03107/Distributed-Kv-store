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

The load balancer:

* Parses the RESP request
* Extracts the key
* Computes its hash
* Routes the request to the correct partition


## Architecture Design

<img width="1344" height="627" alt="Screenshot 2026-05-07 at 1 06 27 AM" src="https://github.com/user-attachments/assets/1c8136e9-77a5-42eb-8987-15cffb2c4bd8" />

## Important Detail

These are **logical partitions**, not independent shard servers:

* Data distribution is based on hashing
* Partitions are pre-defined (no dynamic rebalancing yet)
* Each partition is tightly coupled with its Raft group


## How to Run it 

1. Clone the Repository
2. Start the Backend : 
       cd Backend
       docker compose up (make sure to have docker installed) this will start the nodes and raft consesus between them
3. Start the Frontend:
       cd Frontend
       npm run dev


## Cluster Zoom in

<img width="1291" height="621" alt="Screenshot 2026-05-07 at 1 07 13 AM" src="https://github.com/user-attachments/assets/55cc3a27-49d2-4db8-bccd-af9b0ed7f817" />

## References And For Deeper Understanding
If you want to understand Raft in detail refer to these resources:
  1) Raft Paper : https://raft.github.io/raft.pdf
  2) Raft algorithm Simulator: https://deniz.co/raft-consensus/
  3) Video Reference : https://www.youtube.com/watch?v=uXEYuDwm7e4&t=1790s




