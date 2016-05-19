package kdtree;

public class PointEntry {
	private Point point;
	private float distance;
	
	public PointEntry(Point point, float distance) {
		this.point = point;
		this.distance = distance;
	}
	
	public Point getPoint() {
		return point;
	}

	public float getDistance() {
		return distance;
	}
	
}