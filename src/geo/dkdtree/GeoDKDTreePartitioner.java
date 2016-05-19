package geo.dkdtree;

import org.apache.spark.Partitioner;

public class GeoDKDTreePartitioner extends Partitioner {
	
	private static final long serialVersionUID = -7780152644049352690L;
	int numPartitions;

	public GeoDKDTreePartitioner(int numPartitions) {
		super();
		this.numPartitions = numPartitions;
	}

	@Override
	public int getPartition(Object arg0) {
		// TODO Auto-generated method stub
		Integer obj = (Integer) arg0;
		return obj;
	}

	@Override
	public int numPartitions() {
		// TODO Auto-generated method stub
		return numPartitions;
	}

}
