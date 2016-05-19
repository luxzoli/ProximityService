package geo.dkdtree;

import java.util.ArrayList;
import java.util.PriorityQueue;

import org.apache.spark.api.java.function.PairFunction;

import geo.kdtree.GeoPoint;
import geo.kdtree.GeoPointEntry;
import geo.kdtree.GeoPointEntryComparator;
import scala.Tuple2;

@SuppressWarnings("serial")
public class KNN3 implements
		PairFunction<Tuple2<Long, Iterable<GeoPoint>>, Integer, GeoPoint> {
	private int k = 10;

	public KNN3(int k) {
		this.k = k;
	}

	@Override
	public Tuple2<Integer, GeoPoint> call(Tuple2<Long, Iterable<GeoPoint>> arg0)
			throws Exception {

		PriorityQueue<GeoPointEntry> kNearestNeighbors = new PriorityQueue<GeoPointEntry>(
				k, new GeoPointEntryComparator());
		float kDistance = Float.MAX_VALUE;
		ArrayList<GeoPoint> pointsAL = new ArrayList<GeoPoint>();
		for (GeoPoint p : arg0._2) {
			pointsAL.add(p);
		}
		for (GeoPoint p : pointsAL) {
			for (GeoPoint o : p.getNearestNeighbors()) {
				float distance = GeoPoint.surfaceDistance(o, p);
				if (kDistance > distance) {
					GeoPointEntry pe = new GeoPointEntry(o, distance);
					kNearestNeighbors.offer(pe);
					if (kNearestNeighbors.size() > k) {
						kDistance = kNearestNeighbors.peek().getDistance();
						GeoPointEntry[] tempRemoved = new GeoPointEntry[kNearestNeighbors
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
					GeoPointEntry pe = new GeoPointEntry(o, distance);
					kNearestNeighbors.offer(pe);
				}
			}
		}
		kDistance = kNearestNeighbors.peek().getDistance();
		GeoPoint[] knn = new GeoPoint[kNearestNeighbors.size()];
		int size = kNearestNeighbors.size();
		for (int j = 0; j < size; j++) {
			knn[j] = kNearestNeighbors.poll().getPoint();
		}
		GeoPoint p = pointsAL.get(0);
		p.setkDistance(kDistance);
		p.setNearestNeighbors(knn);
		p.setReady(true);
		pointsAL.clear();
		return new Tuple2<Integer, GeoPoint>(p.getCellID(), p);
	}
}
