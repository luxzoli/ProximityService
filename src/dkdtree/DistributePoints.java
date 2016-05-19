package dkdtree;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import kdtree.KDTreeTop;
import kdtree.Point;
import scala.Tuple2;

public class DistributePoints implements
		PairFlatMapFunction<Tuple2<Integer, Point>, Integer, Point> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6566425207653423334L;
	private KDTreeTop grid;
	private float epsilon;

	public DistributePoints(KDTreeTop grid) {
		this.grid = grid;
		this.epsilon = 0.0f;
		// System.out.println(grid);
	}

	public DistributePoints(KDTreeTop grid, float epsilon) {
		this.grid = grid;
		this.epsilon = epsilon;
		// System.out.println(grid);
	}

	@Override
	public Iterable<Tuple2<Integer, Point>> call(Tuple2<Integer, Point> arg0)
			throws Exception {
		Point p = arg0._2;
		// System.out.println(grid);
		ArrayList<KDTreeTop> matching = grid.getMatchingGrids(p, epsilon);
		List<Tuple2<Integer, Point>> results = new ArrayList<Tuple2<Integer, Point>>();
		for (KDTreeTop m : matching) {
			results.add(new Tuple2<Integer, Point>(new Integer(m.getID()), p));
		}
		return results;
	}

}
