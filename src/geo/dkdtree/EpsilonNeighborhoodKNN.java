package geo.dkdtree;

import java.util.ArrayList;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import geo.kdtree.GeoKDTree;
import geo.kdtree.GeoPoint;
import scala.Tuple2;

@SuppressWarnings("serial")
public class EpsilonNeighborhoodKNN
		implements
		PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<GeoPoint>, GeoKDTree>>, Integer, GeoPoint> {
	private int k;
	private float epsilon;

	public EpsilonNeighborhoodKNN(int k, float epsilon) {
		this.k = k;
		this.epsilon = epsilon;
	}

	@Override
	public Iterable<Tuple2<Integer, GeoPoint>> call(
			Tuple2<Integer, Tuple2<Iterable<GeoPoint>, GeoKDTree>> arg0)
			throws Exception {
		ArrayList<GeoPoint> pointsAL = new ArrayList<GeoPoint>();
		for (GeoPoint p : arg0._2._1) {
			pointsAL.add(p);
		}
		for (GeoPoint point : pointsAL) {
			point.setCellID(arg0._1);
		}
		GeoKDTree tree = arg0._2._2;
		ArrayList<GeoPoint> neighbors = new ArrayList<GeoPoint>();
		ArrayList<Tuple2<Integer, GeoPoint>> results = new ArrayList<Tuple2<Integer, GeoPoint>>();
		System.out.println("here starts the search with a list of " + pointsAL.size() + " elements");
		for (GeoPoint point : pointsAL) {
			float kDistance = GeoKDTree.epsilonNeighborhoodKNNQuery(tree, point,
					k, epsilon, neighbors);
			point.setkDistance(kDistance);
			GeoPoint[] knn = new GeoPoint[neighbors.size()];
			int j = 0;
			for ( GeoPoint q : neighbors) {
				knn[j] = q;
				j++;
			}
			point.setNearestNeighbors(knn);
			point.setReady(true);
			results.add(new Tuple2<Integer, GeoPoint>(new Integer(arg0._1), point));
			neighbors.clear();
		}
		System.out.println("here ends the search with a list of " + pointsAL.size() + " elements");
		return results;
	}

}
