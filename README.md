# Sketch

**Sketch** is a personal research project exploring the design of a vector storage engine. The goal is to prototype ideas, validate assumptions, and experiment with alternative design choices in a controlled, greenfield environment.

## Code Status

This is a research and experimentation project. The code is not intended for production use and can be discarded once the main technical questions are answered and the overall system design becomes clearer.

Code quality reflects its purpose: it is a continuous work in progress aimed at testing ideas, learning, and exploring trade-offs rather than delivering a polished or stable system.

## Motivation

I have participated in the development of several vector databases that run in production environments. Naturally, those systems were shaped by real-world constraints that limited design freedom.

This project is an opportunity to start from scratch and explore what an “ideal” design might look like (at least from my perspective): one that prioritizes performance and efficiency while minimizing resource usage, without being constrained by existing architectural decisions.

## Main Design Decisions

### Specialized Storage Engine

I have seen vector data stored on top of general-purpose storage engines such as B-tree–based or LSM-tree–based systems. While these approaches have solid justifications and work well in production, they come with notable inefficiencies for vector workloads.

For example:
- Storing 6 KB vectors in 16 KB B-tree pages results in roughly 25% unnecessary I/O.
- Managing fixed-size vectors in LSM-tree memtables and SSTables requires significant overhead due to sorting and compaction.

Vector data has specific characteristics:
- Fixed-size records
- Often loaded in batches
- Frequently accessed sequentially
- More OLAP-like than OLTP in many use cases

These properties suggest that a specialized storage layout can be more efficient. Applying full transactional machinery such as WAL to immutable, batch-loaded vector data often provides limited value.

This project explores a storage engine that writes vectors sequentially into data files, optimized for fast sequential scans while still providing indexes for direct record access.

### Out-of-Process Storage Engine

Storage engines are typically implemented as in-process libraries linked directly into a database executable. In this project, I am experimenting with an out-of-process model.

The storage engine runs as a standalone process, accessed through a lightweight adapter library integrated into the database. In the simplest form, this could be implemented as a C extension exposing a user-defined function that returns a dataset.

For example:
- In PostgreSQL, this would be a Set-Returning Function (SRF).
- In SQLite, a Table-Valued Function.

Communicating with a sidecar process over a domain socket and exchanging results for KNN or ANN queries is relatively straightforward and allows for clean separation of concerns.

### Memory Management

The project follows a philosophy similar to LMDB’s: rely on the operating system’s page cache and kernel-level memory management rather than implementing a custom buffer pool.

There are valid reasons not to take this approach, but for a research prototype it significantly reduces complexity and allows focus on higher-level design questions.

If better ideas for memory management emerge later, this decision can be revisited.

### Threading Model

I considered a thread-per-core model, which often provides excellent performance and efficiency. However, adopting it would require implementing a custom buffer pool manager, which I wanted to avoid at this stage.

Instead, the current threading model is intentionally simple:
- One thread per client connection
- One or more worker thread pools for processing requests split into concurrent tasks

Multiple thread pools may exist for different workloads, such as:
- Online query processing
- Background index building

The data layout is designed to allow concurrent processing of different data segments across threads.

### Concurrency Model

The system uses a classic multiple-readers, single-writer concurrency model.

- Data is loaded in batches from local files.
- While data loading is in progress, no other operations are allowed.
- Queries and index building can run concurrently, as they do not modify existing data.
- Index creation produces new indexes without altering existing ones.

### Dependencies

LMDB is used for storing index data. Its support for a single-key, multiple-values data model is particularly useful for implementing IVF-style index structures.
