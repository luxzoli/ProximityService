package geo.dkdtree;

import java.util.ArrayList;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import geo.kdtree.GeoKDTreeTop;
import geo.kdtree.GeoPoint;
import scala.Tuple2;

@SuppressWarnings("serial")
public class KNN2FlatMap implements
		PairFlatMapFunction<Tuple2<Integer, GeoPoint>, Integer, GeoPoint> {
	private GeoKDTreeTop grid;

	public KNN2FlatMap(GeoKDTreeTop grid) {
		this.grid = grid;
	}

	@Override
	public Iterable<Tuple2<Integer, GeoPoint>> call(Tuple2<Integer, GeoPoint> arg0)
			throws Exception {
		ArrayList<Tuple2<Integer, GeoPoint>> results = new ArrayList<Tuple2<Integer, GeoPoint>>();
		GeoPoint p = arg0._2;
		if (!p.isReady()) {
			ArrayList<GeoKDTreeTop> matching = grid.getMatchingGrids(p,
					p.getkDistance());
			for (GeoKDTreeTop m : matching) {
				// lehet fölösleges
				p.setCellID(arg0._1);
				results.add(new Tuple2<Integer, GeoPoint>(m.getID(), p));
			}
		}else {
			results.add(new Tuple2<Integer, GeoPoint>(p.getCellID(), p));
		}	
		return results;
	}

}
