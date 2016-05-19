package geo.kdtree;

import java.util.Comparator;

public class GeoPointEntryComparator implements Comparator<GeoPointEntry> {

	@Override
	public int compare(GeoPointEntry o1, GeoPointEntry o2) {
		return -Float.compare(o1.getDistance(),o2.getDistance());
	}

}
