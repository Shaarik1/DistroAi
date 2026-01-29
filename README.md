# DistroAI: A Lightweight Distributed Runtime Engine

**DistroAI** is a custom-built distributed computing framework that allows Python code to be executed transparently across a cluster of machines. Inspired by systems like Ray and PyTorch Distributed, it handles task scheduling, object serialization, and peer-to-peer data transfer from scratch.

![Architecture Status](https://img.shields.io/badge/Architecture-Master%2FWorker-blue)
![Tech Stack](https://img.shields.io/badge/Tech-gRPC%20%7C%20Protobuf%20%7C%20Python-green)

## 🏗 Architecture

The system consists of three core components:

1.  **Head Node (The Scheduler):**
    * Maintains the global state of the cluster.
    * Tracks active workers via heartbeats.
    * Routes tasks to available workers using Round-Robin scheduling.

2.  **Worker Nodes (The Executors):**
    * Listen for tasks via gRPC.
    * Deserialize Python functions (closures) using `cloudpickle`.
    * Execute code in isolated environments.
    * Store results in local memory for peer-to-peer fetching.

3.  **Client SDK:**
    * Provides a `@remote` decorator to transparently ship functions to the cluster.
    * Connects directly to Workers to fetch results (bypassing the Head Node bottleneck).

## 🚀 Getting Started

### Prerequisites
* Python 3.8+
* `grpcio`, `grpcio-tools`, `cloudpickle`

### Installation
```bash
pip install grpcio grpcio-tools cloudpickle