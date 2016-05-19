package kdtree;

import java.util.Comparator;

public class PointEntryComparator implements Comparator<PointEntry> {

	@Override
	public int compare(PointEntry o1, PointEntry o2) {
		return -Float.compare(o1.getDistance(),o2.getDistance());
	}

}
