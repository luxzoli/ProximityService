package dkdtree;

import org.apache.spark.Partitioner;

public class DKDTreePartitioner extends Partitioner {
	
	private static final long serialVersionUID = -1074147779173651396L;
	int numPartitions;

	public DKDTreePartitioner(int numPartitions) {
		super();
		this.numPartitions = numPartitions;
	}

	@Override
	public int getPartition(Object arg0) {
		Integer obj = (Integer) arg0;
		return obj;
	}

	@Override
	public int numPartitions() {
		return numPartitions;
	}

}
