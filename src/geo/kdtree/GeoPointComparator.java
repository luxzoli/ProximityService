package geo.kdtree;

import java.util.Comparator;

public class GeoPointComparator implements Comparator<GeoPoint> {
	private int actualDimension;

	public GeoPointComparator(int actualDimension) {
		this.actualDimension = actualDimension;
	}

	@Override
	public int compare(GeoPoint o1, GeoPoint o2) {
		return o1.getP()[actualDimension] > o2.getP()[actualDimension] ? 1 : o1
				.getP()[actualDimension] == o2.getP()[actualDimension] ? 0 : -1;
	}

	public int getActualDimension() {
		return actualDimension;
	}

	public void setActualDimension(int actualDimension) {
		this.actualDimension = actualDimension;
	}

}
