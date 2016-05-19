package dkdtree;

import java.util.ArrayList;
import java.util.PriorityQueue;

import org.apache.spark.api.java.function.PairFunction;

import kdtree.Point;
import kdtree.PointEntry;
import kdtree.PointEntryComparator;
import scala.Tuple2;

@SuppressWarnings("serial")
public class KNN3 implements
		PairFunction<Tuple2<Long, Iterable<Point>>, Integer, Point> {
	private int k = 10;

	public KNN3(int k) {
		this.k = k;
	}

	@Override
	public Tuple2<Integer, Point> call(Tuple2<Long, Iterable<Point>> arg0)
			throws Exception {

		PriorityQueue<PointEntry> kNearestNeighbors = new PriorityQueue<PointEntry>(
				k, new PointEntryComparator());
		float kDistance = Float.MAX_VALUE;
		ArrayList<Point> pointsAL = new ArrayList<Point>();
		for (Point p : arg0._2) {
			pointsAL.add(p);
		}
		for (Point p : pointsAL) {
			for (Point o : p.getNearestNeighbors()) {
				float distance = Point.euclideanDistance(o, p);
				if (kDistance > distance) {
					PointEntry pe = new PointEntry(o, distance);
					kNearestNeighbors.offer(pe);
					if (kNearestNeighbors.size() > k) {
						kDistance = kNearestNeighbors.peek().getDistance();
						PointEntry[] tempRemoved = new PointEntry[kNearestNeighbors
								.size()];
						int index = 0;
						while (Math.abs(kNearestNeighbors.peek().getDistance()
								- kDistance) < 0.000001) {
							tempRemoved[index] = kNearestNeighbors.poll();
							index++;
							// if(kNearestNeighbors.size() == 0){
							// break;
							// }
						}
						if (kNearestNeighbors.size() < k) {
							for (int j = 0; j < index; j++) {
								kNearestNeighbors.add(tempRemoved[j]);
							}
						} else {
							kDistance = kNearestNeighbors.peek().getDistance();
						}
					} else if (kNearestNeighbors.size() == k) {
						kDistance = kNearestNeighbors.peek().getDistance();
					}
				} else if (Math.abs(kDistance - distance) < 0.000001) {
					PointEntry pe = new PointEntry(o, distance);
					kNearestNeighbors.offer(pe);
				}
			}
		}
		kDistance = kNearestNeighbors.peek().getDistance();
		Point[] knn = new Point[kNearestNeighbors.size()];
		int size = kNearestNeighbors.size();
		for (int j = 0; j < size; j++) {
			knn[j] = kNearestNeighbors.poll().getPoint();
		}
		Point p = pointsAL.get(0);
		p.setkDistance(kDistance);
		p.setNearestNeighbors(knn);
		p.setReady(true);
		pointsAL.clear();
		return new Tuple2<Integer, Point>(p.getCellID(), p);
	}
}
