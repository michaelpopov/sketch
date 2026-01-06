# sketch
Personal research project to build a sketch of a vector storage engine.

# Code Status
It's a research project. Something that can be thrown away when main technical questions are answered and a clarity about the system design is achieved. Thus, the code quality is not on a production level. It is a continuous "work in progress" intended to experiment with technical ideas, learn new things and find good/acceptable answers to technical questions.

# Reasons for development
I participated in development of a few vector databases. These systems are running in production so there were reasonable constraints on the design decisions. I wanted to experiment with a "green field" development and to figure out the "ideal" design (in my opinion, of course) that delivers the best performance on the least resource usage.

# Main Design Decisions
## Specialized Storage Engine
I've seen systems storing vector data in the existing storage engines: B-tree based and LSM tree based. There were good reasons to use this approach but it incurs high cost of resource usage and adds significant inefficiencies. Storing 6K vectors in 16K B-tree pages causes to 25% unncessary I/O transfers. Storing 6K vectors in LSM-tree memtables and sorting them out in SSTable looks like very heavy lifting. It is obvious that such systems can work in production but the issue with efficiency remains.

There are special properties to vector's data. It's a fixed size record kind of data. It should be possible to define a storage that utilizes this constraint and achives better efficiency. It is also more like OLAP kind of data, which can be loaded to the storage, thus applying transactional mechanisms like WAL to this data seems unnecessary. In many use-case scenarios there is a sequential access to the data, so optimizing for scanning record after record looks like a good idea.

Thus, this sketch presents implemenation of these ideas. It's a storage engine that saves vectors sequentially in data files. This should provide efficient data acccess for sequential scans of data. It also includes indexes to allow direct access to records. It actually uses a third-party component to store and access indexes: LMDB, which is by itself a storage engine. Some key technical ideas from LMDB are also implemented for storing main body of data.

## Out of Process Storage Engine
Noramlly storage engines are implemented as libraries, which are linked into a database executable and run in the same process. I decided to experiment with "out of process" storage engine: the storage engine is running as a standalone process and there is an adapter library integrated into the database that provides access to this storage engine.

TO BE CONTINUED...
