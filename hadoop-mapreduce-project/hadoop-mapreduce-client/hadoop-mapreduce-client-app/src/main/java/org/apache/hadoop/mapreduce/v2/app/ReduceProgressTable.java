package org.apache.hadoop.mapreduce.v2.app;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
/**
 * Created by ganyi on 9/26/2016.
 */
public class ReduceProgressTable {
    private int numReduceTasks;
    private int totalPartitions;
    private ReduceTable[] reduceTables;

    public ReduceProgressTable(int numReduceTasks, int totalPartitions) {
        this.numReduceTasks = numReduceTasks;
        this.totalPartitions = totalPartitions;
        this.reduceTables = new ReduceTable[numReduceTasks];
        for (int reduceId = 0; reduceId < numReduceTasks; reduceId++) {
            reduceTables[reduceId] = new ReduceTable(reduceId, partitionsInReduceTask(reduceId));
        }
    }

    public void markFinished(int reduceId, int localPartitionId) {
        if (localPartitionId == -1) {
            return;
        }
        markFinished(localToGlobal(reduceId, localPartitionId));
    }

    public void markFinished(int globalPartitionId) {
        if (globalPartitionId == -1) {
            return;
        }
        int realReduceId = globalPartitionIdToReduceId(globalPartitionId);
        ReduceTable realReduceTable = reduceTables[realReduceId];
        realReduceTable.markFinished(globaltolocal(realReduceId, globalPartitionId));
    }

    public int nextGlobalPartitionId(int reduceId) {
        ReduceTable selfReduceTable = reduceTables[reduceId];
        int selfNextLocalPartitionId = selfReduceTable.nextLocalPartitionId();
        if (selfNextLocalPartitionId >= 0) {
            return localToGlobal(reduceId, selfNextLocalPartitionId);
        }
        int nextLocalPartitionId = -1;
        for (int i = 0; i < numReduceTasks; i++) {
            if (i == reduceId) {
                continue;
            }
            nextLocalPartitionId = reduceTables[i].nextLocalPartitionId();
            if (nextLocalPartitionId >= 0) {
                return localToGlobal(i, nextLocalPartitionId);
            }
        }
        return -1;
    }

    private int globaltolocal(int reduceId, int globalPartitionId) {
        return globalPartitionId - beginPartitionId(reduceId);
    }

    private int localToGlobal(int reduceId, int localPartitionId) {
        return beginPartitionId(reduceId) + localPartitionId;
    }

    private int partitionsInReduceTask(int reduceId) {
        int average = totalPartitions / numReduceTasks;
        return reduceId == numReduceTasks - 1 ? average + (totalPartitions % numReduceTasks) : average;
    }

    private int beginPartitionId(int reduceId) {
        return (totalPartitions / numReduceTasks) * reduceId;
    }

    private int globalPartitionIdToReduceId(int globalPartitionId) {
        return numReduceTasks*globalPartitionId /totalPartitions ;
    }

    enum PartitionState {
        UNASSIGNED, RUNNING, COMPLETE
    }

    // for each Reduce
    class ReduceTable {
        final int reduceId;
        final int numPartitions;
        PartitionState[] partitions;
        int curIndex; /* point to first UNASSIGNED partition */

        ReduceTable(int reduceId, int numPartitions) {
            this.reduceId = reduceId;
            this.numPartitions = numPartitions;
            this.partitions = new PartitionState[numPartitions];
        }
        public synchronized void markFinished(int localPartitionId) {
            if (localPartitionId < 0 || localPartitionId >= numPartitions) {
                return;
            }
            assert partitions[localPartitionId] == PartitionState.RUNNING;
            partitions[localPartitionId] = PartitionState.COMPLETE;
        }
        public synchronized int nextLocalPartitionId() {
            if (curIndex == numPartitions) {
                return -1;
            }
            assert partitions[curIndex] == PartitionState.UNASSIGNED;
            int chosenIndex = curIndex;
            partitions[chosenIndex] = PartitionState.RUNNING;
            curIndex++;
            return chosenIndex;
        }
    }
}
