package dkdtree;

import java.util.ArrayList;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import kdtree.KDTree;
import kdtree.Point;
import scala.Tuple2;

@SuppressWarnings("serial")
public class EpsilonNeighborhoodKNN
		implements
		PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Point>, KDTree>>, Integer, Point> {
	private int k;
	private float epsilon;

	public EpsilonNeighborhoodKNN(int k, float epsilon) {
		this.k = k;
		this.epsilon = epsilon;
	}

	@Override
	public Iterable<Tuple2<Integer, Point>> call(
			Tuple2<Integer, Tuple2<Iterable<Point>, KDTree>> arg0)
			throws Exception {
		ArrayList<Point> pointsAL = new ArrayList<Point>();
		for (Point p : arg0._2._1) {
			pointsAL.add(p);
		}
		for (Point point : pointsAL) {
			point.setCellID(arg0._1);
		}
		KDTree tree = arg0._2._2;
		ArrayList<Point> neighbors = new ArrayList<Point>();
		ArrayList<Tuple2<Integer, Point>> results = new ArrayList<Tuple2<Integer, Point>>();
		for (Point point : pointsAL) {
			float kDistance = KDTree.epsilonNeighborhoodKNNQuery(tree, point,
					k, epsilon, neighbors);
			point.setkDistance(kDistance);
			Point[] knn = new Point[neighbors.size()];
			for (int j = 0; j < neighbors.size(); j++) {
				knn[j] = neighbors.get(j);
			}
			point.setNearestNeighbors(knn);
			point.setReady(true);
			results.add(new Tuple2<Integer, Point>(new Integer(arg0._1), point));
			neighbors.clear();
		}
		return results;
	}

}
