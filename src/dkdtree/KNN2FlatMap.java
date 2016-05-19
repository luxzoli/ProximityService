package dkdtree;

import java.util.ArrayList;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import kdtree.KDTreeTop;
import kdtree.Point;
import scala.Tuple2;

@SuppressWarnings("serial")
public class KNN2FlatMap implements
		PairFlatMapFunction<Tuple2<Integer, Point>, Integer, Point> {
	private KDTreeTop grid;

	public KNN2FlatMap(KDTreeTop grid) {
		this.grid = grid;
	}

	@Override
	public Iterable<Tuple2<Integer, Point>> call(Tuple2<Integer, Point> arg0)
			throws Exception {
		ArrayList<Tuple2<Integer, Point>> results = new ArrayList<Tuple2<Integer, Point>>();
		Point p = arg0._2;
		if (!p.isReady()) {
			ArrayList<KDTreeTop> matching = grid.getMatchingGrids(p,
					p.getkDistance());
			for (KDTreeTop m : matching) {
				// lehet fölösleges
				p.setCellID(arg0._1);
				results.add(new Tuple2<Integer, Point>(m.getID(), p));
			}
		}else {
			results.add(new Tuple2<Integer, Point>(p.getCellID(), p));
		}	
		return results;
	}

}
