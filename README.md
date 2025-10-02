# Distributed-sorting-with-fault-tolerance
Implement a distributed merge sort algorithm, where the data is partitioned and sorted by different nodes, and the results are merged. Add fault tolerance so that if one node goes down, others can continue sorting.

# My Assumptions:
Master Node: Responsible for partitioning data, distributing chunks, managing node failures, and merging results.
Worker Nodes: Sort the assigned chunks and send the sorted results back to the master node.
Fault tolerance strategy: Reassign work from a failed node to another. 

# My Approach:

1.     Partitioning and Distribution:

o    The master node divides the data into N chunks and assigns each chunk to a worker node.

o    Data partitioning can be based on a simple equal chunks distribution strategy.

2.     Sorting at Worker Nodes:

o    Each worker node performs merge sort on its chunk of data.

o    After sorting, the sorted chunks are sent back to the master node.

3.     Merging Sorted Results:

o    The master node receives sorted chunks from each worker.

o    A distributed merge process combines the sorted chunks into a final sorted array.

4.     Fault Tolerance:

o   If a node fails, the master detects the failure and reassigns the unsorted chunk to another available node.


