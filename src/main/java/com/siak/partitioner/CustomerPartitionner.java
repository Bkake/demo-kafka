package com.siak.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class CustomerPartitionner implements Partitioner {

    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes,
                         Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if ((keyBytes == null) || (!(key instanceof String))) {
            throw new InvalidRecordException("We expect all messages " +
                    "to have customer name as key");
        }

        if ( key.equals("id_1") || key.equals("id_5")) {
            return numPartitions - 1; // Banana will always go to last partition
            // Other records will get hashed to the rest of the partitions
        }
        return Math.abs(Utils.murmur2(keyBytes)) % (numPartitions - 1);
    }

    public void close() {
       // Do nothing
    }

    public void configure(Map<String, ?> map) {
      // Do nothing
    }
}
