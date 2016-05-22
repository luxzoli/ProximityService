package dkdtree;

import java.io.Serializable;

import kdtree.Point;

public class BoundaryObject implements Serializable{
	
	private static final long serialVersionUID = 3769813104862257874L;
	private Point min;
	private Point max;
	private long count;

	public BoundaryObject(Point min, Point max) {
		super();
		this.min = new Point(min);
		this.max = new Point(max);
		setCount(1);
	}

	public Point getMin() {
		return min;
	}

	public void setMin(Point min) {
		this.min = min;
	}

	public Point getMax() {
		return max;
	}

	public void setMax(Point max) {
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
