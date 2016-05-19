package geo.dkdtree;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import geo.kdtree.GeoKDTreeTop;
import geo.kdtree.GeoPoint;
import scala.Tuple2;

public class DistributePointsToSingle implements
		PairFlatMapFunction<Tuple2<Integer, GeoPoint>, Integer, GeoPoint> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6566425207653423334L;
	private GeoKDTreeTop grid;
	private float epsilon;

	public DistributePointsToSingle(GeoKDTreeTop grid) {
		this.grid = grid;
		this.epsilon = 0.0f;
		// System.out.println(grid);
	}

	public DistributePointsToSingle(GeoKDTreeTop grid, float epsilon) {
		this.grid = grid;
		this.epsilon = epsilon;
		// System.out.println(grid);
	}

	@Override
	public Iterable<Tuple2<Integer, GeoPoint>> call(Tuple2<Integer, GeoPoint> arg0)
			throws Exception {
		GeoPoint p = arg0._2;
		ArrayList<GeoKDTreeTop> matching = grid.getMatchingGrids(p, epsilon);
		List<Tuple2<Integer, GeoPoint>> results = new ArrayList<Tuple2<Integer, GeoPoint>>();
		GeoKDTreeTop m = matching.get(0);
		results.add(new Tuple2<Integer, GeoPoint>(new Integer(m.getID()), p));
		return results;
	}

}
