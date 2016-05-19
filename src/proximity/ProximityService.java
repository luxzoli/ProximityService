package proximity;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaDStream;

import geo.dkdtree.GeoDKDTree;
import geo.kdtree.GeoPoint;

public class ProximityService {
	public static JavaDStream<GeoPoint> queryNearby(JavaDStream<GeoPoint> pointsStream, JavaRDD<GeoPoint> dataset, int k,
			float epsilon, int numPartitions, int sampleSize) {
		JavaDStream<GeoPoint> res = GeoDKDTree.streamEpsilonNeighborhoodKNNQuery(pointsStream, dataset, k, epsilon,
				numPartitions, sampleSize);
		return res;
	}

}
