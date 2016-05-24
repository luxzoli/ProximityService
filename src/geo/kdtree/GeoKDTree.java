package geo.kdtree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

@SuppressWarnings("serial")
public class GeoKDTree implements Serializable {
	
	private int maxLeafSize;
	private int count = 0;
	private GeoPoint[] points;
	private boolean isLeaf;
	private boolean isRoot = false;
	private GeoKDTree left;
	private GeoKDTree right;
	private int leafID;
	private float[] lowerBoundary;
	private float[] upperBoundary;
	private static int numLeaves;
	private static final float R = 6371000; // Radius of the earth in meters

	static {
		numLeaves = 0;
	}

	@SuppressWarnings("unused")
	private GeoKDTree() {

	}

	public GeoKDTree(GeoPoint[] points, int maxLeafSize) {
		GeoKDTree.numLeaves = 0;
		this.isRoot = true;
		this.setCount(points.length);
		this.maxLeafSize = maxLeafSize;
		this.setID(0);
		this.lowerBoundary = new float[2];
		this.upperBoundary = new float[2];
		GeoKDTree.findBoundaries(points, lowerBoundary, upperBoundary);
		if (points.length <= this.maxLeafSize
				|| GeoKDTree.sameBoundaries(lowerBoundary, upperBoundary)) {
			this.setPoints(points);
			isLeaf = true;
			numLeaves++;
			this.leafID = numLeaves - 1;
			return;
		}
		int actualDimension = 0;
		// other possibility is median of medians
		GeoPointComparator pc = new GeoPointComparator(actualDimension);
		Arrays.sort(points, pc);
		int half = (points.length / 2) + (points.length % 2) - 1;
		GeoPoint median = calculateMedian(points, half);
		int leftSize = half + 1;
		while (points[leftSize + 1] == points[leftSize]) {
			leftSize++;
		}
		GeoPoint[] leftArr = new GeoPoint[leftSize];
		GeoPoint[] rightArr = new GeoPoint[points.length - leftSize];
		for (int i = 0; i < leftSize; i++) {
			leftArr[i] = points[i];
		}
		for (int i = leftSize; i < points.length; i++) {
			rightArr[i - leftSize] = points[i];
		}
		float[] leftUpperBoundary = new float[upperBoundary.length];
		float[] rightLowerBoundary = new float[lowerBoundary.length];
		calculateBoundaries(lowerBoundary, upperBoundary, actualDimension,
				median, leftUpperBoundary, rightLowerBoundary);
		int d = 2;
		points = null;
		left = new GeoKDTree(leftArr, maxLeafSize, (actualDimension + 1) % d,
				lowerBoundary, leftUpperBoundary);
		right = new GeoKDTree(rightArr, maxLeafSize, (actualDimension + 1) % d,
				rightLowerBoundary, upperBoundary);
		isLeaf = false;
		this.leafID = -1;
	}

	private GeoKDTree(GeoPoint[] points, int maxLeafSize, int actualDimension,
			float[] lowerBoundary, float[] upperBoundary) {
		this.setCount(points.length);
		this.maxLeafSize = maxLeafSize;
		this.setID(0);
		this.lowerBoundary = lowerBoundary;
		this.upperBoundary = upperBoundary;
		if (points.length <= this.maxLeafSize
				|| GeoKDTree.sameBoundaries(lowerBoundary, upperBoundary)) {
			this.setPoints(points);
			isLeaf = true;
			numLeaves++;
			this.leafID = numLeaves - 1;
			return;
		}
		// other possibility is median of medians
		GeoPointComparator pc = new GeoPointComparator(actualDimension);
		Arrays.sort(points, pc);
		int half = points.length / 2 + points.length % 2 - 1;
		GeoPoint median = calculateMedian(points, half);
		int leftSize = half + 1;
		while (points[leftSize + 1] == points[half]) {
			leftSize++;
		}
		GeoPoint[] leftArr = new GeoPoint[leftSize];
		GeoPoint[] rightArr = new GeoPoint[points.length - leftSize];
		for (int i = 0; i < leftSize; i++) {
			leftArr[i] = points[i];
		}
		for (int i = leftSize; i < points.length; i++) {
			rightArr[i - leftSize] = points[i];
		}
		float[] leftUpperBoundary = new float[upperBoundary.length];
		float[] rightLowerBoundary = new float[lowerBoundary.length];
		calculateBoundaries(lowerBoundary, upperBoundary, actualDimension,
				median, leftUpperBoundary, rightLowerBoundary);
		int d = 2;
		points = null;
		left = new GeoKDTree(leftArr, maxLeafSize, (actualDimension + 1) % d,
				lowerBoundary, leftUpperBoundary);
		right = new GeoKDTree(rightArr, maxLeafSize, (actualDimension + 1) % d,
				rightLowerBoundary, upperBoundary);
		isLeaf = false;
		this.leafID = -1;
	}

	private void calculateBoundaries(float[] lowerBoundary,
			float[] upperBoundary, int actualDimension, GeoPoint median,
			float[] leftUpperBoundary, float[] rightLowerBoundary) {
		for (int i = 0; i < upperBoundary.length; i++) {
			if (i == actualDimension) {
				leftUpperBoundary[i] = rightLowerBoundary[i] = median.getP()[i];
			} else {
				leftUpperBoundary[i] = upperBoundary[i];
				rightLowerBoundary[i] = lowerBoundary[i];
			}
		}
	}

	public boolean isInside(GeoPoint point, float epsilon) {
		float[] coords = point.getP();
		for (int i = 0; i < this.lowerBoundary.length; i++) {
			if ((coords[i] >= (this.lowerBoundary[i] - epsilon))
					&& (coords[i] <= (this.upperBoundary[i] + epsilon))) {
				continue;
			} else {
				return false;
			}
		}
		return true;
	}

	public boolean isInside(GeoPoint point) {
		float[] coords = point.getP();
		for (int i = 0; i < this.lowerBoundary.length; i++) {
			if ((coords[i] >= this.lowerBoundary[i])
					&& (coords[i] <= this.upperBoundary[i])) {
				continue;
			} else {
				return false;
			}
		}
		return true;
	}

	public static boolean isInRange(GeoPoint point, float[] lowerBoundary,
			float[] upperBoundary) {
		float[] coords = point.getP();
		for (int i = 0; i < coords.length; i++) {
			if (coords[i] < lowerBoundary[i] || coords[i] > upperBoundary[i]) {
				return false;
			}
		}
		return true;
	}

	public ArrayList<GeoKDTree> getMatchingGrids(GeoPoint point, float epsilon) {
		ArrayList<GeoKDTree> matching = new ArrayList<GeoKDTree>();
		if (this.isLeaf) {
			if (isInside(point, epsilon)) {
				matching.add(this);
			}
		} else {
			if (left.isInside(point, epsilon)) {
				matching.addAll(left.getMatchingGrids(point, epsilon));
			}
			if (right.isInside(point, epsilon)) {
				matching.addAll(right.getMatchingGrids(point, epsilon));
			}
		}
		return matching;
	}

	public static int getNumLeaves() {
		return numLeaves;
	}

	public int getID() {
		return leafID;
	}

	public void setID(int ID) {
		this.leafID = ID;
	}

	public float[] getLowerBoundary() {
		return lowerBoundary;
	}

	public void setLowerBoundary(float[] lowerBoundary) {
		this.lowerBoundary = lowerBoundary;
	}

	public float[] getUpperBoundary() {
		return upperBoundary;
	}

	public void setUpperBoundary(float[] upperBoundary) {
		this.upperBoundary = upperBoundary;
	}

	public String toString() {
		String gridAsString = new String();
		String prefix = "{";
		String suffix = "}";
		gridAsString += prefix;
		if (isRoot) {
			gridAsString += "root,";
			gridAsString += maxLeafSize + ",";
			gridAsString += Arrays.toString(lowerBoundary).replace(" ", "")
					+ ",";
			gridAsString += Arrays.toString(upperBoundary).replace(" ", "")
					+ ";";
			if (this.left != null)
				gridAsString += this.left.toString() + ";";
			if (this.right != null)
				gridAsString += this.right.toString();
			gridAsString += suffix;
			return gridAsString;
		} else if (isLeaf) {
			gridAsString += "leaf,";
			gridAsString += Arrays.toString(lowerBoundary).replace(" ", "")
					+ ",";
			gridAsString += Arrays.toString(upperBoundary).replace(" ", "");
			gridAsString += suffix;
			return gridAsString;
		} else {
			gridAsString += "inner_node,";
			gridAsString += Arrays.toString(lowerBoundary).replace(" ", "")
					+ ",";
			gridAsString += Arrays.toString(upperBoundary).replace(" ", "")
					+ ";";
			gridAsString += this.left.toString() + ";";
			gridAsString += this.right.toString();
			gridAsString += suffix;
			return gridAsString;
		}
	}

	public static int findClosure(String gridAsString, char start, char end) {
		int count = 0;
		int endIndex = -1;
		for (int i = 0; i < gridAsString.length(); i++) {
			if (gridAsString.charAt(i) == start) {
				count++;
			} else if (gridAsString.charAt(i) == end) {
				count--;
			}
			if (count == 0) {
				endIndex = i;
				return endIndex;
			}
		}
		// System.out.println(endIndex);
		return endIndex;
	}

	public static float[] parseBoundary(String boundaryString) {
		// System.out.println(boundaryString);
		String nums = boundaryString.replace("[", "").replace("]", "");
		StringTokenizer st = new StringTokenizer(nums, ",");
		float[] boundaryArr = new float[st.countTokens()];
		int numTokens = st.countTokens();
		for (int i = 0; i < numTokens; i++) {
			boundaryArr[i] = Float.parseFloat(st.nextToken());
		}
		return boundaryArr;
	}

	public GeoPoint[] getPoints() {
		return points;
	}

	public void setPoints(GeoPoint[] points) {
		this.points = points;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public static boolean sameBoundaries(float[] lowerBoundary,
			float[] upperBoundary) {
		for (int i = 0; i < lowerBoundary.length; i++) {
			if (lowerBoundary[i] != upperBoundary[i]) {
				return false;
			}
		}
		return true;
	}

	public GeoPoint calculateMedian(GeoPoint[] points, int half) {
		int d = 2;
		float[] medianArr = new float[d];
		float[] medianArrLower = points[half].getP();
		float[] medianArrUpper = points[half + ((half + 1) % 2)].getP();
		for (int i = 0; i < d; i++) {
			medianArr[i] = (medianArrLower[i] + medianArrUpper[i]) / 2;
		}
		return new GeoPoint(medianArr);
	}

	public static void rangeSearch(GeoKDTree node, float[] lowerBoundary,
			float[] upperBoundary, ArrayList<GeoPoint> nodesInRange) {
		rangeSearch(node, lowerBoundary, upperBoundary, nodesInRange, 0);
	}

	private static void rangeSearch(GeoKDTree node, float[] lowerBoundary,
			float[] upperBoundary, ArrayList<GeoPoint> nodesInRange, int level) {
		int d = node.lowerBoundary.length;
		if (upperBoundary[level] < node.lowerBoundary[level]) {
			return;
		} else if (lowerBoundary[level] > node.upperBoundary[level]) {
			return;
		}

		if (node.isLeaf) {
			for (GeoPoint point : node.getPoints()) {
				if (GeoKDTree.isInRange(point, lowerBoundary, upperBoundary)) {
					nodesInRange.add(point);
				}
			}
		} else {
			rangeSearch(node.left, lowerBoundary, upperBoundary, nodesInRange,
					(level + 1) % d);
			rangeSearch(node.right, lowerBoundary, upperBoundary, nodesInRange,
					(level + 1) % d);
		}
	}

	public static void epsilonNeighborhood(GeoKDTree node, GeoPoint p, float epsilon,
			ArrayList<GeoPoint> neighbors) {
		int d = p.getD();
		float[] coords = p.getP();
		float[] upperBoundary = new float[d];
		float[] lowerBoundary = new float[d];
		for (int i = 0; i < d; i++) {
			upperBoundary[i] = coords[i] + epsilon;
			lowerBoundary[i] = coords[i] - epsilon;
		}
		ArrayList<GeoPoint> nodesInRange = new ArrayList<GeoPoint>();
		rangeSearch(node, lowerBoundary, upperBoundary, nodesInRange);
		float epsilonDistance = (float) ((epsilon / 180.f) * R * (2 * Math.PI));
		for (GeoPoint o : nodesInRange) {
			if (GeoPoint.approximateDistance(p, o) <= epsilonDistance) {
				neighbors.add(o);
			}
		}
	}

	public static float kNNQuery(GeoKDTree node, GeoPoint p, int k,
			ArrayList<GeoPoint> kNearestNeighbors) {
		float kDistance = Float.MAX_VALUE;
		PriorityQueue<GeoPointEntry> pq = new PriorityQueue<GeoPointEntry>(k,
				new GeoPointEntryComparator());
		kDistance = kNNQuery(node, p, k, kDistance, pq, 0);
		for (GeoPointEntry pe : pq) {
			GeoPoint point = pe.getPoint();
			kNearestNeighbors.add(point);
		}
		return kDistance;
	}

	public static float epsilonNeighborhoodKNNQuery(GeoKDTree node, GeoPoint p,
			int k, float epsilon, ArrayList<GeoPoint> kNearestNeighbors) {
		float kDistance = Float.MAX_VALUE;
		PriorityQueue<GeoPointEntry> pq = new PriorityQueue<GeoPointEntry>(k,
				new GeoPointEntryComparator());
		kDistance = kNNQuery(node, p, k, kDistance, pq, 0);
		float epsilonDistance = (float) ((epsilon / 180.f) * R * (2 * Math.PI));
		for (GeoPointEntry pe : pq) {
			GeoPoint point = pe.getPoint();
			if (p.approximateDistance(point) <= epsilonDistance) {
				kNearestNeighbors.add(point);
			}
		}
		return kDistance;
	}

	// csak akkor jó, ha a pont is benne van a fában
	public static float exclusiveKNNQuery(GeoKDTree node, GeoPoint p, int k,
			ArrayList<GeoPoint> kNearestNeighbors) {
		float kDistance = Float.MAX_VALUE;
		PriorityQueue<GeoPointEntry> pq = new PriorityQueue<GeoPointEntry>(k,
				new GeoPointEntryComparator());
		kDistance = kNNQuery(node, p, k + 1, kDistance, pq, 0);
		while (pq.size() > 1) {
			GeoPointEntry pe = pq.poll();
			GeoPoint point = pe.getPoint();
			kNearestNeighbors.add(point);
		}
		return kDistance;
	}

	public static float kDistance(GeoKDTree node, GeoPoint p, int k) {
		float kDistance = Float.MAX_VALUE;
		PriorityQueue<GeoPointEntry> pq = new PriorityQueue<GeoPointEntry>(k,
				new GeoPointEntryComparator());
		return kNNQuery(node, p, k, kDistance, pq, 0);
	}

	public static float exclusiveKDistance(GeoKDTree node, GeoPoint p, int k) {
		float kDistance = Float.MAX_VALUE;
		PriorityQueue<GeoPointEntry> pq = new PriorityQueue<GeoPointEntry>(k,
				new GeoPointEntryComparator());
		return kNNQuery(node, p, k + 1, kDistance, pq, 0);
	}

	private static float kNNQuery(GeoKDTree node, GeoPoint p, int k, float kDistance,
			PriorityQueue<GeoPointEntry> kNearestNeighbors, int level) {
		if (node.isLeaf) {
			for (GeoPoint o : node.getPoints()) {
				float distance = GeoPoint.approximateDistance(o, p);
				if ((kDistance - distance) > 0.000001) {
					GeoPointEntry pe = new GeoPointEntry(o, distance);
					kNearestNeighbors.offer(pe);
					if (kNearestNeighbors.size() > k) {
						GeoPointEntry[] tempRemoved = new GeoPointEntry[kNearestNeighbors
								.size()];
						kDistance = kNearestNeighbors.peek().getDistance();
						int index = 0;
						while (Math.abs(kNearestNeighbors.peek().getDistance()
								- kDistance) < 0.000001) {
							tempRemoved[index] = kNearestNeighbors.poll();
							index++;
						}
						if (kNearestNeighbors.size() < k) {
							for (int i = 0; i < index; i++) {
								kNearestNeighbors.add(tempRemoved[i]);
							}
						} else {
							kDistance = kNearestNeighbors.peek().getDistance();
						}
					} else if (kNearestNeighbors.size() == k) {
						kDistance = kNearestNeighbors.peek().getDistance();
					}
				} else if (Math.abs(kDistance - distance) < 0.000001) {
					GeoPointEntry pe = new GeoPointEntry(o, distance);
					kNearestNeighbors.offer(pe);
				}
			}
			return kDistance;
		}
		int d = node.lowerBoundary.length;
		float pLevCoord;
		if ((level % 2) == 0) {
			pLevCoord = p.getLat();
		} else {
			pLevCoord = p.getLon();
		}
		// TODO: ellenoriz distance
		float splitCoord = node.left.upperBoundary[level];
		float distanceToSplit;
		GeoPoint q;
		GeoPoint r;
		switch (level % 2) {
		case 0:
			q = new GeoPoint(0, splitCoord);
			r = new GeoPoint(0, pLevCoord);
			distanceToSplit = GeoPoint.approximateDistance(r, q);
			break;
		case 1:
			q = new GeoPoint(splitCoord, 0);
			r = new GeoPoint(pLevCoord, 0);
			distanceToSplit = GeoPoint.approximateDistance(r, q);
			break;
		//This will never happen but it is necessary for the compiler	
		default:
			q = new GeoPoint(splitCoord, 0);
			r = new GeoPoint(pLevCoord, 0);
			distanceToSplit = GeoPoint.approximateDistance(r, q);
			break;
		}
		boolean leftCloser = pLevCoord <= splitCoord ? true : false;
		if (leftCloser) {
			kDistance = kNNQuery(node.left, p, k, kDistance, kNearestNeighbors,
					(level + 1) % d);
			if ((distanceToSplit - kDistance) < 0.000001
					|| kNearestNeighbors.size() < k) {
				kDistance = kNNQuery(node.right, p, k, kDistance,
						kNearestNeighbors, (level + 1) % d);
			}
		} else {
			kDistance = kNNQuery(node.right, p, k, kDistance,
					kNearestNeighbors, (level + 1) % d);
			if ((distanceToSplit - kDistance) < 0.000001
					|| kNearestNeighbors.size() < k) {
				kDistance = kNNQuery(node.left, p, k, kDistance,
						kNearestNeighbors, (level + 1) % d);
			}
		}
		return kDistance;
	}

	@SuppressWarnings("unused")
	private static float exclusiveKNNQuery(GeoKDTree node, GeoPoint p, int k,
			float kDistance, PriorityQueue<GeoPointEntry> kNearestNeighbors,
			int level) {
		if (node.isLeaf) {
			for (GeoPoint o : node.getPoints()) {
				float distance = GeoPoint.approximateDistance(o, p);
				if (kDistance > distance) {
					GeoPointEntry pe = new GeoPointEntry(o, distance);
					if ((o.getID() != p.getID())
							|| (o.getCellID() != p.getCellID())) {
						kNearestNeighbors.offer(pe);
					}
					if (kNearestNeighbors.size() > k) {
						GeoPointEntry[] tempRemoved = new GeoPointEntry[kNearestNeighbors
								.size()];
						kDistance = kNearestNeighbors.peek().getDistance();
						int index = 0;
						while (kNearestNeighbors.peek().getDistance() == kDistance) {
							tempRemoved[index] = kNearestNeighbors.poll();
							index++;
						}
						if (kNearestNeighbors.size() < k) {
							for (int i = 0; i < index; i++) {
								kNearestNeighbors.add(tempRemoved[i]);
							}
						} else {
							kDistance = kNearestNeighbors.peek().getDistance();
						}
					} else if (kNearestNeighbors.size() == k) {
						kDistance = kNearestNeighbors.peek().getDistance();
					}
				} else if (kDistance == distance) {
					GeoPointEntry pe = new GeoPointEntry(o, distance);
					kNearestNeighbors.offer(pe);
				}
			}
			return kDistance;
		}
		int d = node.lowerBoundary.length;
		float pLevCoord;
		if ((level % 2) == 0) {
			pLevCoord = p.getLat();
		} else {
			pLevCoord = p.getLon();
		}
		float splitCoord = node.left.upperBoundary[level];
		float distanceToSplit;
		GeoPoint q;
		GeoPoint r;
		switch (level % 2) {
		case 0:
			q = new GeoPoint(0, splitCoord);
			r = new GeoPoint(0, pLevCoord);
			distanceToSplit = GeoPoint.approximateDistance(r, q);
			break;
		case 1:
			q = new GeoPoint(splitCoord, 0);
			r = new GeoPoint(pLevCoord, 0);
			distanceToSplit = GeoPoint.approximateDistance(r, q);
			break;
		//This will never happen	
		default:
			q = new GeoPoint(splitCoord, 0);
			r = new GeoPoint(pLevCoord, 0);
			distanceToSplit = GeoPoint.approximateDistance(r, q);
			break;
		}
		boolean leftCloser = pLevCoord <= splitCoord ? true : false;
		if (leftCloser) {
			kDistance = kNNQuery(node.left, p, k, kDistance, kNearestNeighbors,
					(level + 1) % d);
			if (distanceToSplit <= kDistance || kNearestNeighbors.size() < k) {
				kDistance = kNNQuery(node.right, p, k, kDistance,
						kNearestNeighbors, (level + 1) % d);
			}
		} else {
			kDistance = kNNQuery(node.right, p, k, kDistance,
					kNearestNeighbors, (level + 1) % d);
			if (distanceToSplit <= kDistance || kNearestNeighbors.size() < k) {
				kDistance = kNNQuery(node.left, p, k, kDistance,
						kNearestNeighbors, (level + 1) % d);
			}
		}
		return kDistance;
	}

	public static void findBoundaries(GeoPoint[] points, float[] lowerBoundary,
			float[] upperBoundary) {
		for (int i = 0; i < lowerBoundary.length; i++) {
			lowerBoundary[i] = Float.MAX_VALUE;
			upperBoundary[i] = Float.MIN_VALUE;
		}
		for (GeoPoint p : points) {
			float[] coords = p.getP();
			for (int i = 0; i < p.getD(); i++) {
				if (lowerBoundary[i] > coords[i]) {
					lowerBoundary[i] = coords[i];
				}
				if (upperBoundary[i] < coords[i]) {
					upperBoundary[i] = coords[i];
				}
			}
		}
	}

	
	//TODO: nézd át a képleteket
	public static boolean iskDistanceReady(GeoKDTree tree, GeoPoint p, float kDistance) {
		float[] lowerBoundary = tree.lowerBoundary;
		float[] upperBoundary = tree.upperBoundary;

		float lat1 = p.getLat();
		float lon1 = p.getLon();
		float lat2 = lowerBoundary[0];
		float lon2 = lowerBoundary[1];
		float latDistance = (float) ((lat2 - lat1) * Math.PI / 180);
		float lonDistance = (float) ((lon2 - lon1) * Math.PI / 180);
		float a = (float) (Math.sin(latDistance / 2) * Math
				.sin(latDistance / 2) /*
									 * + Math.cos(lat1 * Math.PI / 180)
									 * Math.cos(lat2 * Math.PI / 180) *
									 * Math.sin(0.0f) Math.sin(0.0f)
									 */);

		float c = (float) (2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)));
		float distanceToBoundary = R * c;
		if ((kDistance - distanceToBoundary) > -0.000001) {
			return false;
		}
		a = (float) (/* Math.sin(0.0f) * Math.sin(0.0f) */+Math.cos(lat1
				* Math.PI / 180)
				* Math.cos(lat2 * Math.PI / 180) * Math.sin(lonDistance / 2) * Math
				.sin(lonDistance / 2));
		c = (float) (2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)));
		distanceToBoundary = R * c;

		if ((kDistance - distanceToBoundary) > -0.000001) {
			return false;
		}

		lat2 = upperBoundary[0];
		lon2 = upperBoundary[1];
		latDistance = (float) ((lat2 - lat1) * Math.PI / 180);
		lonDistance = (float) ((lon2 - lon1) * Math.PI / 180);
		a = (float) (Math.sin(latDistance / 2) * Math.sin(latDistance / 2));

		c = (float) (2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)));
		distanceToBoundary = R * c;
		if ((kDistance - distanceToBoundary) > -0.000001) {
			return false;
		}
		a = (float) (/* Math.sin(0.0f) * Math.sin(0.0f) + */Math.cos(lat1
				* Math.PI / 180)
				* Math.cos(lat2 * Math.PI / 180) * Math.sin(lonDistance / 2) * Math
				.sin(lonDistance / 2));
		c = (float) (2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)));
		distanceToBoundary = R * c;

		if ((kDistance - distanceToBoundary) > -0.000001) {
			return false;
		}

		return true;
	}
}
