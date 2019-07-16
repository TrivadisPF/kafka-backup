package ch.tbd.kafka.backuprestore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PartitionSelector {
	private int maxPartitions;
	private int currentPartition;
	private Map<Integer, Integer> repeatPartitions = new HashMap<>();
	private Map<Integer, Integer> countPartitions = new HashMap<>();
	
	public PartitionSelector (int maxPartitions) {
		this.maxPartitions = maxPartitions;
		this.currentPartition = 0;
		
		for (int i = 0; i<maxPartitions; i++) {
			repeatPartitions.put(i, 1);
			countPartitions.put(i, 0);
		}
	}

	public PartitionSelector (int maxPartitions, List<Integer> repeatPerPartition) {
		this.maxPartitions = maxPartitions;
		this.currentPartition = 0;
		
		for (int i = 0; i<maxPartitions; i++) {
			if (i < repeatPerPartition.size()) {
				repeatPartitions.put(i, repeatPerPartition.get(i));
			} else {
				repeatPartitions.put(i, 1);
			}
				
			countPartitions.put(i, 0);
		}
	}

	
	public int nextPartition() {
		int nextPartition;
		// add 1 to count for the current Partition
		countPartitions.put(currentPartition, countPartitions.get(currentPartition) + 1);
		nextPartition = currentPartition;
		if (countPartitions.get(currentPartition) == repeatPartitions.get(currentPartition)) {
			countPartitions.put(currentPartition, 0);
			currentPartition++;
			if (currentPartition == maxPartitions) {
				currentPartition = 0;
			}
		}
		return nextPartition; 	
	}


}