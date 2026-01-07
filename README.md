# sketch
Personal research project to build a sketch of a vector storage engine.

# Code Status
It's a research project. Something that can be thrown away when main technical questions are answered and a clarity about the system design is achieved. Thus, the code quality is not on a production level. It is a continuous "work in progress" intended to experiment with technical ideas, learn new things and find good/acceptable answers to technical questions.

# Reasons for development
I participated in development of a few vector databases. These systems are running in production so there were reasonable constraints on design decisions. I wanted to experiment with a "green field" development and to figure out the "ideal" design (in my opinion, of course) that delivers the best performance on the least resource usage.

# Main Design Decisions
## Specialized Storage Engine
I've seen systems storing vector data in the existing storage engines: B-tree based and LSM tree based. There were good reasons to use this approach but it incurs high cost of resource usage and adds significant inefficiencies. Storing 6K vectors in 16K B-tree pages causes to 25% unncessary I/O transfers. Storing 6K vectors in LSM-tree memtables and sorting them out in SSTable looks like very heavy lifting. It is obvious that such systems can work in production but the issue with efficiency remains.

There are special properties to vector's data. It's a fixed size record data. It should be possible to define a storage that utilizes this constraint and achives better efficiency. It is also more like OLAP kind of data, which can be loaded to the storage as a batch, thus applying transactional mechanisms like WAL to this data seems unnecessary. In many use-case scenarios there is a sequential access to the data, so optimizing for scanning record after record looks like a good idea.

Thus, this sketch presents implemenation of these ideas. It's a storage engine that saves vectors sequentially in data files. This should provide efficient data acccess for sequential scans of data. It also includes indexes to allow direct access to records.

## Out of Process Storage Engine
Noramlly storage engines are implemented as libraries, which are linked into a database executable and run in the same process. I decided to experiment with "out of process" storage engine: the storage engine is running as a standalone process and adding an adapter library integrated into a database executable that provides access to this storage engine.

It can be even simler: create a C module with a user defined function that returns a dataset. In Postgres such functions called Set Returning Functions (SRFs). In SQLite corresponding functions called Table-Valued Functions. Creating a domain socket connection to a side-car process, sending and receiving results of KNN or ANN searches seems trivial.

## Memory Management
This implementation follows LMDB's philosophy: "Very smart people develop the Linux kernel. Let's rely on their work for memory management and use a page cache."
There are many reasons not to do things this way. But for the purposes of development of this prototype of a system offloading concerns regarding memory management to kernel seems prudent.

If one day I have better ideas how to implement memory management for this project, I will do it.

## Threading Model
I considered "thread-per-core" threading model. It seems the best model from the point of view of performance and efficiency. But going this way requires implementing my own buffer pool manager, which I decided to avoid at this stage of development.

Thus, the threading model is very simple:
  - Thread per connection
  - A pool of working treads processing requests split to multiple concurrent tasks.

There will be multiple thread pools. For example, a thread pool for processing online requests and another thread pool for building indexes in the background.

The data organization allows processing parts of the data concurrently on multiple threads.

## Concurrency Model
It's a classical "multiple readers - single writer" model.
Data can be loaded into the system as a batch from a local file. While data is loading, no other tasks can be executed.
Running data queries and building indexes can be done concurrently because these tasks do not change the data. Building a index creates a new index without modifying the existing one.

## Dependencies
I am using LMDB storage engine for storing index data. LMDB supports "single key - multiple values" data model, which was handy for storing IVF index data.


