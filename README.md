# Consistent Hashing Implementation

## Project Overview
This project implements a **Consistent Hashing** algorithm in Python without relying on external libraries. It is designed to solve the scalability issues found in traditional modulo-based hashing (`hash(key) % N`) by ensuring that adding or removing nodes results in minimal data movement.

The system features a **Hash Ring** topology, **Virtual Nodes** for load balancing, and a **Simulation Harness** to measure key redistribution efficiency.

---

## Key Features
* **Deterministic Hashing:** Uses SHA-1 (truncated to 64-bit) to ensure identical outputs for the same input across all clients.
* **Virtual Nodes (Vnodes):** Implements $K$ virtual nodes per physical node to ensure uniform key distribution and minimize standard deviation.
* **Efficient Lookups:** Utilizes binary search (via Python's `bisect` module) to achieve **O(log N)** lookup time.
* **Minimal Data Movement:** Guarantees that adding/removing a node only affects $\approx 1/N$ of the total keys, unlike modulo hashing which affects nearly $100\%$.

---

## Project Structure

| File | Description |
| :--- | :--- |
| `consistent_hashing.py` | Contains the `ConsistentHashRing`, `HashFunction`, and `LoadBalancerSimulator` classes. |
| `README.md` | Project documentation, design decisions, and complexity analysis. |

---

## How to Run

### Prerequisites
* Python 3.6 or higher
* Standard libraries only (`hashlib`, `bisect`, `struct`, `collections`)

### Execution
Run the simulation directly from the terminal:

```bash
python consistent_hashing.py
```

Compile: 
```bash
javac ConsistentHashing.java
```

Run:
```bash
java ConsistentHashing