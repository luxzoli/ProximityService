package geo.kdtree;

public class GeoPointEntry {
	private GeoPoint point;
	private float distance;
	
	public GeoPointEntry(GeoPoint point, float distance) {
		this.point = point;
		this.distance = distance;
	}
	
	public GeoPoint getPoint() {
		return point;
	}

	public float getDistance() {
		return distance;
	}
	
}