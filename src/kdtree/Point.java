package kdtree;

import java.io.Serializable;
import java.util.Arrays;
import java.util.StringTokenizer;

@SuppressWarnings("serial")
public class Point implements Serializable {

	private int d;
	private float[] p;
	private boolean isCorePoint;
	private float kDistance;
	private boolean isReady;
	private Point[] nearestNeighbors = null;
	private long ID;
	private int cellID;

	public Point(float[] p) {
		this.d = p.length;
		this.p = p;
	}

	// TODO: nem teljes
	public Point(Point p) {
		this.d = p.d;
		this.p = new float[p.p.length];
		for (int i = 0; i < p.p.length; i++) {
			this.p[i] = p.p[i];
		}
		this.isReady = p.isReady;
		this.isCorePoint = p.isCorePoint;
		this.kDistance = p.kDistance;
	}

	private Point() {

	}

	public Point(String p, boolean mark) {
		if (p.endsWith("r")) {
			p = p.substring(0, p.length() - 2);
			StringTokenizer st = new StringTokenizer(p, " ");
			this.ID = Long.parseLong(st.nextToken(" "));
			int d = st.countTokens() - 1;
			this.d = d;
			this.p = new float[d];
			for (int i = 0; i < d; i++) {
				String s = st.nextToken();
				this.p[i] = Float.parseFloat(s);
			}
			kDistance = Float.parseFloat(st.nextToken());
			this.isReady = true;
		} else {
			p = p.substring(0, p.length() - 2);
			StringTokenizer stu = new StringTokenizer(p, "|");
			StringTokenizer st = new StringTokenizer(stu.nextToken(), " ");
			int d = st.countTokens() - 1;
			this.d = d;
			this.p = new float[d];
			for (int i = 0; i < d; i++) {
				String s = st.nextToken();
				this.p[i] = Float.parseFloat(s);
			}
			kDistance = Float.parseFloat(st.nextToken());
			this.isReady = false;
			StringTokenizer stn = new StringTokenizer(stu.nextToken(), ";");
			nearestNeighbors = new Point[stn.countTokens()];
			for (int i = 0; stn.hasMoreElements(); i++) {
				String o = stn.nextToken();
				StringTokenizer sto = new StringTokenizer(o, " ");
				float[] coords = new float[d];
				for (int j = 0; j < d; j++) {
					String s = sto.nextToken();
					coords[j] = Float.parseFloat(s);
				}
				Point op = new Point();
				op.p = coords;
				op.d = d;
				nearestNeighbors[i] = op;
			}
		}

	}

	public Point(String p, String delim) {
		StringTokenizer st = new StringTokenizer(p, delim);
		this.ID = Long.parseLong(st.nextToken(delim));
		int d = st.countTokens();
		this.d = d;
		this.p = new float[d];
		for (int i = 0; i < d; i++) {
			String s = st.nextToken();
			this.p[i] = Float.parseFloat(s);
		}
	}

	public Point(String p) {
		StringTokenizer st = new StringTokenizer(p, " ");
		this.ID = Long.parseLong(st.nextToken());
		int d = st.countTokens();
		this.d = d;
		this.p = new float[d];
		for (int i = 0; i < d; i++) {
			String s = st.nextToken();
			this.p[i] = Float.parseFloat(s);
		}
	}

	public Point(String p, String delim, boolean mark) {
		if (p.endsWith("r")) {
			p = p.substring(0, p.length() - 2);
			StringTokenizer st = new StringTokenizer(p, delim);
			this.ID = Long.parseLong(st.nextToken(delim));
			int d = st.countTokens() - 1;
			this.d = d;
			this.p = new float[d];
			for (int i = 0; i < d; i++) {
				String s = st.nextToken();
				this.p[i] = Float.parseFloat(s);
			}
			kDistance = Float.parseFloat(st.nextToken());
			this.isReady = true;
		} else {
			p = p.substring(0, p.length() - 3);
			StringTokenizer stu = new StringTokenizer(p, "|");
			StringTokenizer st = new StringTokenizer(stu.nextToken(), delim);
			this.ID = Long.parseLong(st.nextToken(delim));
			int d = st.countTokens() - 1;
			this.d = d;
			this.p = new float[d];
			for (int i = 0; i < d; i++) {
				String s = st.nextToken();
				this.p[i] = Float.parseFloat(s);
			}
			kDistance = Float.parseFloat(st.nextToken());
			this.isReady = false;
			StringTokenizer stn = new StringTokenizer(stu.nextToken(), ";");
			nearestNeighbors = new Point[stn.countTokens()];
			for (int i = 0; stn.hasMoreElements(); i++) {
				String o = stn.nextToken();
				StringTokenizer sto = new StringTokenizer(o, delim);
				float[] coords = new float[d];
				for (int j = 0; j < d; j++) {
					String s = sto.nextToken();
					coords[j] = Float.parseFloat(s);
				}
				Point op = new Point();
				op.p = coords;
				op.d = d;
				nearestNeighbors[i] = op;
			}
		}
	}

	public static float euclideanDistance(float[] p1, Point p2) {
		/*
		 * if(o.getD() != this.d){ return null; }
		 */
		float distance = 0;
		for (int i = 0; i < p1.length; i++) {
			distance += ((p1[i] - p2.p[i]) * (p1[i] - p2.p[i]));
		}
		distance = (float) Math.sqrt(distance);
		return distance;
	}

	public static float euclideanDistance(Point p1, Point p2) {
		/*
		 * if(o.getD() != this.d){ return null; }
		 */
		float distance = 0;
		for (int i = 0; i < p1.d; i++) {
			distance += ((p1.p[i] - p2.p[i]) * (p1.p[i] - p2.p[i]));
		}
		distance = (float) Math.sqrt(distance);
		return distance;
	}

	public float euclideanDistance(Point o) {
		/*
		 * if(o.getD() != this.d){ return null; }
		 */
		float distance = 0;
		for (int i = 0; i < this.d; i++) {
			distance += ((this.p[i] - o.p[i]) * (this.p[i] - o.p[i]));
		}
		distance = (float) Math.sqrt(distance);
		return distance;
	}

	public int getD() {
		return p.length;
	}

	public float[] getP() {
		return p;
	}

	public void setP(float[] p) {
		this.p = p;
	}

	@Override
	public String toString() {
		String point = ID
				+ " "
				+ Arrays.toString(p).replace(",", "").replace("[", "")
						.replace("]", "") + " " + kDistance;
		String neighbors = "|";
		if (!isReady) {
			for (Point neighbor : nearestNeighbors) {
				neighbors += Arrays.toString(neighbor.p).replace(",", "")
						.replace("[", "").replace("]", "")
						+ ";";
			}
			return point + neighbors + " n";
		} else {
			return point + " r";
		}
	}

	public String toSimpleString() {
		return ID
				+ " "
				+ Arrays.toString(p).replace(",", "").replace("[", "")
						.replace("]", "");
	}

	public String toCoordsString() {
		return Arrays.toString(p).replace(",", "").replace("[", "")
				.replace("]", "");
	}

	public boolean isCorePoint() {
		return isCorePoint;
	}

	public void setCorePoint(boolean isCorePoint) {
		this.isCorePoint = isCorePoint;
	}

	public float getkDistance() {
		return kDistance;
	}

	public void setkDistance(float kDistance) {
		this.kDistance = kDistance;
	}

	public boolean isReady() {
		return isReady;
	}

	public void setReady(boolean isReady) {
		this.isReady = isReady;
	}

	public Point[] getNearestNeighbors() {
		return nearestNeighbors;
	}

	public void setNearestNeighbors(Point[] nearestNeighbors) {
		if (nearestNeighbors == null) {
			this.nearestNeighbors = null;
			return;
		}
		this.nearestNeighbors = new Point[nearestNeighbors.length];
		for (int i = 0; i < nearestNeighbors.length; i++) {
			this.nearestNeighbors[i] = new Point(nearestNeighbors[i]);
		}
	}

	public int getCellID() {
		return cellID;
	}

	public void setCellID(int cellID) {
		this.cellID = cellID;
	}

	public long getID() {
		return ID;
	}

	public void setID(long ID) {
		this.ID = ID;
	}

}
