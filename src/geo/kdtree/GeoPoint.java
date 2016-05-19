package geo.kdtree;

import java.io.Serializable;
import java.util.Arrays;
import java.util.StringTokenizer;

@SuppressWarnings("serial")
public class GeoPoint implements Serializable {
	
	private static final float R = 6371000; // Radius of the earth in meters

	private float lat;
	private float lon;
	private boolean isCorePoint;
	private float kDistance;
	private boolean isReady = true;
	private GeoPoint[] nearestNeighbors = null;
	private long ID;
	private int cellID;

	public GeoPoint(float lat, float lon) {
		this.lat = lat;
		this.lon = lon;
	}

	public GeoPoint(float[] p) {
		this.lat = p[0];
		this.lon = p[1];
	}

	// TODO: nem teljes, kiegeszitve nem teljesrol, mi legyen?
	public GeoPoint(GeoPoint p) {
		this.lat = p.lat;
		this.lon = p.lon;
		this.isReady = p.isReady;
		this.isCorePoint = p.isCorePoint;
		this.kDistance = p.kDistance;
		//kiegeszites
		this.isReady = p.isReady;
		this.nearestNeighbors = p.nearestNeighbors;
		//kiegeszites
		this.ID = p.ID;
		this.cellID = p.cellID;
	}

	private GeoPoint() {

	}

	public GeoPoint(String p) {
		if (p.endsWith("r")) {
			p = p.substring(0, p.length() - 2);
			StringTokenizer st = new StringTokenizer(p, " ");
			this.ID = Long.parseLong(st.nextToken(" "));

			String s = st.nextToken();
			this.setLat(Float.parseFloat(s));
			s = st.nextToken();
			this.setLon(Float.parseFloat(s));

			kDistance = Float.parseFloat(st.nextToken());
			this.isReady = true;
		} else {
			p = p.substring(0, p.length() - 2);
			StringTokenizer stu = new StringTokenizer(p, "|");
			StringTokenizer st = new StringTokenizer(stu.nextToken(), " ");
			String s = st.nextToken();
			this.setLat(Float.parseFloat(s));
			s = st.nextToken();
			this.setLon(Float.parseFloat(s));
			kDistance = Float.parseFloat(st.nextToken());
			this.isReady = false;
			StringTokenizer stn = new StringTokenizer(stu.nextToken(), ";");
			nearestNeighbors = new GeoPoint[stn.countTokens()];
			for (int i = 0; stn.hasMoreElements(); i++) {
				String o = stn.nextToken();
				StringTokenizer sto = new StringTokenizer(o, " ");
				GeoPoint op = new GeoPoint();
				op.setLat(Float.parseFloat(sto.nextToken()));
				op.setLon(Float.parseFloat(sto.nextToken()));
				nearestNeighbors[i] = op;
			}
		}

	}

	public GeoPoint(String p, String delim, boolean mark) {
		StringTokenizer st = new StringTokenizer(p, delim);
		this.ID = Long.parseLong(st.nextToken(delim));
		String s = st.nextToken();
		this.setLat(Float.parseFloat(s));
		s = st.nextToken();
		this.setLon(Float.parseFloat(s));
	}

	public GeoPoint(String p, boolean mark) {
		StringTokenizer st = new StringTokenizer(p, " ");
		this.ID = Long.parseLong(st.nextToken());
		String s = st.nextToken();
		this.setLat(Float.parseFloat(s));
		s = st.nextToken();
		this.setLon(Float.parseFloat(s));
	}

	public GeoPoint(String p, String delim) {
		if (p.endsWith("r")) {
			p = p.substring(0, p.length() - 2);
			StringTokenizer st = new StringTokenizer(p, delim);
			this.ID = Long.parseLong(st.nextToken(delim));
			String s = st.nextToken();
			this.setLat(Float.parseFloat(s));
			s = st.nextToken();
			this.setLon(Float.parseFloat(s));
			kDistance = Float.parseFloat(st.nextToken());
			this.isReady = true;
		} else {
			p = p.substring(0, p.length() - 3);
			StringTokenizer stu = new StringTokenizer(p, "|");
			StringTokenizer st = new StringTokenizer(stu.nextToken(), delim);
			this.ID = Long.parseLong(st.nextToken(delim));
			String s = st.nextToken();
			this.setLat(Float.parseFloat(s));
			s = st.nextToken();
			this.setLon(Float.parseFloat(s));
			kDistance = Float.parseFloat(st.nextToken());
			this.isReady = false;
			StringTokenizer stn = new StringTokenizer(stu.nextToken(), ";");
			nearestNeighbors = new GeoPoint[stn.countTokens()];
			for (int i = 0; stn.hasMoreElements(); i++) {
				String o = stn.nextToken();
				StringTokenizer sto = new StringTokenizer(o, " ");
				GeoPoint op = new GeoPoint();
				op.setLat(Float.parseFloat(sto.nextToken()));
				op.setLon(Float.parseFloat(sto.nextToken()));
				nearestNeighbors[i] = op;
			}
		}
	}

	public static float surfaceDistance(float[] p1, GeoPoint p2) {
		final float R = 6371000; // Radius of the earth in meters
		float lat1 = p1[0];
		float lon1 = p1[1];
		float lat2 = p2.getLat();
		float lon2 = p2.getLon();
		float latDistance = (float) ((lat2 - lat1) * Math.PI / 180);
		float lonDistance = (float) ((lon2 - lon1) * Math.PI / 180);
		float a = (float) (Math.sin(latDistance / 2)
				* Math.sin(latDistance / 2) + Math.cos(lat1 * Math.PI / 180)
				* Math.cos(lat2 * Math.PI / 180) * Math.sin(lonDistance / 2)
				* Math.sin(lonDistance / 2));
		float c = (float) (2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)));
		float distance = R * c;
		return distance;
	}

	public static float surfaceDistance(float lat, float lon, GeoPoint p2) {
		final float R = 6371000; // Radius of the earth in meters
		float lat1 = lat;
		float lon1 = lon;
		float lat2 = p2.getLat();
		float lon2 = p2.getLon();
		float latDistance = (float) ((lat2 - lat1) * Math.PI / 180);
		float lonDistance = (float) ((lon2 - lon1) * Math.PI / 180);
		float a = (float) (Math.sin(latDistance / 2)
				* Math.sin(latDistance / 2) + Math.cos(lat1 * Math.PI / 180)
				* Math.cos(lat2 * Math.PI / 180) * Math.sin(lonDistance / 2)
				* Math.sin(lonDistance / 2));
		float c = (float) (2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)));
		float distance = R * c;
		return distance;
	}

	public static float surfaceDistance(GeoPoint p1, GeoPoint p2) {
		final float R = 6371000; // Radius of the earth in meters
		float lat1 = p1.getLat();
		float lon1 = p1.getLon();
		float lat2 = p2.getLat();
		float lon2 = p2.getLon();
		float latDistance = (float) ((lat2 - lat1) * Math.PI / 180);
		float lonDistance = (float) ((lon2 - lon1) * Math.PI / 180);
		float a = (float) (Math.sin(latDistance / 2)
				* Math.sin(latDistance / 2) + Math.cos(lat1 * Math.PI / 180)
				* Math.cos(lat2 * Math.PI / 180) * Math.sin(lonDistance / 2)
				* Math.sin(lonDistance / 2));
		float c = (float) (2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)));
		float distance = R * c;
		return distance;
	}

	public float surfaceDistance(GeoPoint o) {
		final float R = 6371000; // Radius of the earth in meters
		float lat1 = this.getLat();
		float lon1 = this.getLon();
		float lat2 = o.getLat();
		float lon2 = o.getLon();
		float latDistance = (float) ((lat2 - lat1) * Math.PI / 180);
		float lonDistance = (float) ((lon2 - lon1) * Math.PI / 180);
		float a = (float) (Math.sin(latDistance / 2)
				* Math.sin(latDistance / 2) + Math.cos(lat1 * Math.PI / 180)
				* Math.cos(lat2 * Math.PI / 180) * Math.sin(lonDistance / 2)
				* Math.sin(lonDistance / 2));
		float c = (float) (2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)));
		float distance = R * c;
		return distance;
	}

	public static float surfaceDistanceFast(GeoPoint p1, GeoPoint p2) {
		final float R = 6371000; // Radius of the earth in meters
		float lat1 = p1.getLat();
		float lon1 = p1.getLon();
		float lat2 = p2.getLat();
		float lon2 = p2.getLon();
		float latDistance = (float) ((lat2 - lat1) * Math.PI / 180);
		float lonDistance = (float) ((lon2 - lon1) * Math.PI / 180);
		float a = (float) (Math.sin(latDistance / 2)
				* Math.sin(latDistance / 2) + Math.cos(lat1 * Math.PI / 180)
				* Math.cos(lat2 * Math.PI / 180) * Math.sin(lonDistance / 2)
				* Math.sin(lonDistance / 2));
		float c = (float) (2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)));
		float distance = R * c;
		return distance;
	}

	public float surfaceDistanceFast(GeoPoint o) {
		final float R = 6371000; // Radius of the earth in meters
		float lat1 = (float) (this.getLat() * Math.PI / 180);
		float lon1 = (float) (this.getLon() * Math.PI / 180);
		float lat2 = (float) (o.getLat() * Math.PI / 180);
		float lon2 = (float) (o.getLon() * Math.PI / 180);
		float lonDistance = (float) ((lon2 - lon1));
		float distance = (float) (Math.acos(Math.sin(lat1) * Math.sin(lat2)
				+ Math.cos(lat1) * Math.cos(lat2) * Math.cos(lonDistance)) * R);
		return distance;
	}

	// TODO: fast equirectangular approximation
	public float approximateDistance(GeoPoint o) {
		float lat1 = (float) (this.lat * Math.PI / 180);
		float lon1 = (float) (this.lon * Math.PI / 180);
		float lat2 = (float) (o.lat * Math.PI / 180);
		float lon2 = (float) (o.lon * Math.PI / 180);
		float x = (float) ((lon2 - lon1) * Math.cos((lat1 + lat2) / 2));
		float y = (lat2 - lat1);
		float distance = (float) (Math.sqrt(x * x + y * y) * R);
		return distance;
	}

	public static float approximateDistance(GeoPoint p1, GeoPoint p2) {
		float lat1 = (float) (p1.lat * Math.PI / 180);
		float lon1 = (float) (p1.lon * Math.PI / 180);
		float lat2 = (float) (p2.lat * Math.PI / 180);
		float lon2 = (float) (p2.lon * Math.PI / 180);
		float x = (float) ((lon2 - lon1) * Math.cos((lat1 + lat2) / 2));
		float y = (lat2 - lat1);
		float distance = (float) (Math.sqrt(x * x + y * y) * R);
		return distance;
	}

	public int getD() {
		return 2;
	}

	public float[] getP() {
		return new float[] { getLat(), getLon() };
	}

	public void setP(float[] p) {
		this.setLat(p[0]);
		this.setLon(p[1]);
	}

	@Override
	public String toString() {
		String point = ID
				+ " "
				+ Arrays.toString(this.getP()).replace(",", "")
						.replace("[", "").replace("]", "") + " " + kDistance;
		String neighbors = "|";

		if (!isReady) {
			if (this.nearestNeighbors != null) {
				for (GeoPoint neighbor : nearestNeighbors) {
					neighbors += Arrays.toString(neighbor.getP())
							.replace(",", "").replace("[", "").replace("]", "")
							+ ";";
				}
			}
			return point + neighbors + " n";
		} else {
			return point + " r";
		}
	}

	public String toSimpleString() {
		return ID
				+ " "
				+ Arrays.toString(this.getP()).replace(",", "")
						.replace("[", "").replace("]", "");
	}

	public String toCoordsString() {
		return Arrays.toString(this.getP()).replace(",", "").replace("[", "")
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

	public GeoPoint[] getNearestNeighbors() {
		return nearestNeighbors;
	}

	public void setNearestNeighbors(GeoPoint[] nearestNeighbors) {
		if (nearestNeighbors == null) {
			this.nearestNeighbors = null;
			return;
		}
		this.nearestNeighbors = new GeoPoint[nearestNeighbors.length];
		for (int i = 0; i < nearestNeighbors.length; i++) {
			this.nearestNeighbors[i] = new GeoPoint(nearestNeighbors[i]);
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

	public float getLat() {
		return lat;
	}

	public void setLat(float lat) {
		this.lat = lat;
	}

	public float getLon() {
		return lon;
	}

	public void setLon(float lon) {
		this.lon = lon;
	}

}
