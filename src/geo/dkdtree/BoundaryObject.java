package geo.dkdtree;

import java.io.Serializable;

import geo.kdtree.GeoPoint;

public class BoundaryObject implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 3769813104862257874L;
	private GeoPoint min;
	private GeoPoint max;
	private long count;

	public BoundaryObject(GeoPoint min, GeoPoint max) {
		super();
		this.min = new GeoPoint(min);
		this.max = new GeoPoint(max);
		setCount(1);
	}

	public GeoPoint getMin() {
		return min;
	}

	public void setMin(GeoPoint min) {
		this.min = min;
	}

	public GeoPoint getMax() {
		return max;
	}

	public void setMax(GeoPoint max) {
		this.max = max;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	@Override
	public String toString() {
		return "BoundaryObject [min=" + min.toSimpleString() + ", max=" + max.toSimpleString() + ", count="
				+ count + "]";
	}

}
