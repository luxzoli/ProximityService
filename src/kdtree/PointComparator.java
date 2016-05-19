package kdtree;

import java.util.Comparator;

public class PointComparator implements Comparator<Point> {
	private int actualDimension;

	public PointComparator(int actualDimension) {
		this.actualDimension = actualDimension;
	}

	@Override
	public int compare(Point o1, Point o2) {
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
