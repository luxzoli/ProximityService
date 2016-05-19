package geo.dkdtree;

import java.util.ArrayList;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import geo.kdtree.GeoKDTree;
import geo.kdtree.GeoPoint;
import scala.Tuple2;

@SuppressWarnings("serial")
public class KNN1
		implements
		PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<GeoPoint>, GeoKDTree>>, Integer, GeoPoint> {
	private int k;

	public KNN1(int k) {
		this.k = k;
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
			// lehet nem kell
			point.setCellID(arg0._1);
			//
		}
		GeoKDTree tree = arg0._2._2;
		ArrayList<GeoPoint> neighbors = new ArrayList<GeoPoint>();
		ArrayList<Tuple2<Integer, GeoPoint>> results = new ArrayList<Tuple2<Integer, GeoPoint>>();
		for (GeoPoint point : pointsAL) {
			float kDistance = GeoKDTree.exclusiveKNNQuery(tree, point, k,
					neighbors);
			point.setkDistance(kDistance);
			GeoPoint[] knn = new GeoPoint[neighbors.size()];
			for (int j = 0; j < neighbors.size(); j++) {
				knn[j] = neighbors.get(j);
			}
			point.setNearestNeighbors(knn);
			if (GeoKDTree.iskDistanceReady(tree, point, kDistance)) {
				point.setReady(true);
			} else {
				point.setReady(false);
				// point.setNearestNeighbors(knn);
			}
			// context.write(key, new Text(p.toString() + "#" + p.getID() + " "
			// + cellID));
			results.add(new Tuple2<Integer, GeoPoint>(new Integer(arg0._1), point));
			neighbors.clear();
		}
		return results;
	}

}
