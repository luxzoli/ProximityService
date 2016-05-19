package kdtree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

@SuppressWarnings("serial")
public class KDTree implements Serializable {
	private static int maxLeafSize;
	private int count = 0;
	private Point[] points;
	private boolean isLeaf;
	private boolean isRoot = false;
	private KDTree left;
	private KDTree right;
	private int leafID;
	private float[] lowerBoundary;
	private float[] upperBoundary;
	private static int numLeaves;

	
	static {
		numLeaves = 0;
	}

	@SuppressWarnings("unused")
	private KDTree() {

	}

	public KDTree(Point[] points, int maxLeafSize) {
		KDTree.numLeaves = 0;
		this.isRoot = true;
		this.setCount(points.length);
		KDTree.maxLeafSize = maxLeafSize;
		this.setID(0);
		this.lowerBoundary = new float[points[0].getD()];
		this.upperBoundary = new float[points[0].getD()];
		KDTree.findBoundaries(points, lowerBoundary, upperBoundary);
		if (points.length <= KDTree.maxLeafSize
				|| KDTree.sameBoundaries(lowerBoundary, upperBoundary)) {
			this.setPoints(points);
			isLeaf = true;
			numLeaves++;
			this.leafID = numLeaves - 1;
			return;
		}
		int actualDimension = 0;
		// other possibility is median of medians
		PointComparator pc = new PointComparator(actualDimension);
		Arrays.sort(points, pc);
		int half = (points.length / 2) + (points.length % 2) - 1;
		Point median = calculateMedian(points, half);
		int leftSize = half + 1;
		while (points[leftSize + 1] == points[leftSize]) {
			leftSize++;
		}
		Point[] leftArr = new Point[leftSize];
		Point[] rightArr = new Point[points.length - leftSize];
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
		int d = points[0].getD();
		points = null;
		left = new KDTree(leftArr, maxLeafSize, (actualDimension + 1) % d,
				lowerBoundary, leftUpperBoundary);
		right = new KDTree(rightArr, maxLeafSize, (actualDimension + 1) % d,
				rightLowerBoundary, upperBoundary);
		isLeaf = false;
		this.leafID = -1;
	}

	private KDTree(Point[] points, int maxLeafSize, int actualDimension,
			float[] lowerBoundary, float[] upperBoundary) {
		this.setCount(points.length);
		KDTree.maxLeafSize = maxLeafSize;
		this.setID(0);
		this.lowerBoundary = lowerBoundary;
		this.upperBoundary = upperBoundary;
		if (points.length <= KDTree.maxLeafSize
				|| KDTree.sameBoundaries(lowerBoundary, upperBoundary)) {
			this.setPoints(points);
			isLeaf = true;
			numLeaves++;
			this.leafID = numLeaves - 1;
			return;
		}
		// other possibility is median of medians
		PointComparator pc = new PointComparator(actualDimension);
		Arrays.sort(points, pc);
		int half = points.length / 2 + points.length % 2 - 1;
		Point median = calculateMedian(points, half);
		int leftSize = half + 1;
		while (points[leftSize + 1] == points[half]) {
			leftSize++;
		}
		Point[] leftArr = new Point[leftSize];
		Point[] rightArr = new Point[points.length - leftSize];
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
		int d = points[0].getD();
		points = null;
		left = new KDTree(leftArr, maxLeafSize, (actualDimension + 1) % d,
				lowerBoundary, leftUpperBoundary);
		right = new KDTree(rightArr, maxLeafSize, (actualDimension + 1) % d,
				rightLowerBoundary, upperBoundary);
		isLeaf = false;
		this.leafID = -1;
	}

	private void calculateBoundaries(float[] lowerBoundary,
			float[] upperBoundary, int actualDimension, Point median,
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

	public boolean isInside(Point point, float epsilon) {
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

	public boolean isInside(Point point) {
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

	public static boolean isInRange(Point point, float[] lowerBoundary,
			float[] upperBoundary) {
		float[] coords = point.getP();
		for (int i = 0; i < coords.length; i++) {
			if (coords[i] < lowerBoundary[i] || coords[i] > upperBoundary[i]) {
				return false;
			}
		}
		return true;
	}

	public ArrayList<KDTree> getMatchingGrids(Point point, float epsilon) {
		ArrayList<KDTree> matching = new ArrayList<KDTree>();
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

	public Point[] getPoints() {
		return points;
	}

	public void setPoints(Point[] points) {
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

	public Point calculateMedian(Point[] points, int half) {
		int d = points[0].getP().length;
		float[] medianArr = new float[d];
		float[] medianArrLower = points[half].getP();
		float[] medianArrUpper = points[half + ((half + 1) % 2)].getP();
		for (int i = 0; i < d; i++) {
			medianArr[i] = (medianArrLower[i] + medianArrUpper[i]) / 2;
		}
		return new Point(medianArr);
	}

	public static void rangeSearch(KDTree node, float[] lowerBoundary,
			float[] upperBoundary, ArrayList<Point> nodesInRange) {
		rangeSearch(node, lowerBoundary, upperBoundary, nodesInRange, 0);
	}

	private static void rangeSearch(KDTree node, float[] lowerBoundary,
			float[] upperBoundary, ArrayList<Point> nodesInRange, int level) {
		int d = node.lowerBoundary.length;
		if (upperBoundary[level] < node.lowerBoundary[level]) {
			return;
		} else if (lowerBoundary[level] > node.upperBoundary[level]) {
			return;
		}

		if (node.isLeaf) {
			for (Point point : node.getPoints()) {
				if (KDTree.isInRange(point, lowerBoundary, upperBoundary)) {
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

	public static void epsilonNeighborhood(KDTree node, Point p, float epsilon,
			ArrayList<Point> neighbors) {
		int d = p.getD();
		float[] coords = p.getP();
		float[] upperBoundary = new float[d];
		float[] lowerBoundary = new float[d];
		for (int i = 0; i < d; i++) {
			upperBoundary[i] = coords[i] + epsilon;
			lowerBoundary[i] = coords[i] - epsilon;
		}
		ArrayList<Point> nodesInRange = new ArrayList<Point>();
		rangeSearch(node, lowerBoundary, upperBoundary, nodesInRange);
		for (Point o : nodesInRange) {
			if (Point.euclideanDistance(p, o) <= epsilon) {
				neighbors.add(o);
			}
		}
	}

	public static float kNNQuery(KDTree node, Point p, int k,
			ArrayList<Point> kNearestNeighbors) {
		float kDistance = Float.MAX_VALUE;
		PriorityQueue<PointEntry> pq = new PriorityQueue<PointEntry>(k,
				new PointEntryComparator());
		kDistance = kNNQuery(node, p, k, kDistance, pq, 0);
		for (PointEntry pe : pq) {
			Point point = pe.getPoint();
			kNearestNeighbors.add(point);
		}
		return kDistance;
	}

	public static float epsilonNeighborhoodKNNQuery(KDTree node, Point p,
			int k, float epsilon, ArrayList<Point> kNearestNeighbors) {
		float kDistance = Float.MAX_VALUE;
		PriorityQueue<PointEntry> pq = new PriorityQueue<PointEntry>(k,
				new PointEntryComparator());
		kDistance = kNNQuery(node, p, k, kDistance, pq, 0);
		for (PointEntry pe : pq) {
			Point point = pe.getPoint();
			if (p.euclideanDistance(point) <= epsilon) {
				kNearestNeighbors.add(point);
			}
		}
		return kDistance;
	}

	// csak akkor jó, ha a pont is benne van a fában
	public static float exclusiveKNNQuery(KDTree node, Point p, int k,
			ArrayList<Point> kNearestNeighbors) {
		float kDistance = Float.MAX_VALUE;
		PriorityQueue<PointEntry> pq = new PriorityQueue<PointEntry>(k,
				new PointEntryComparator());
		kDistance = kNNQuery(node, p, k + 1, kDistance, pq, 0);
		while (pq.size() > 1) {
			PointEntry pe = pq.poll();
			Point point = pe.getPoint();
			kNearestNeighbors.add(point);
		}
		return kDistance;
	}

	public static float kDistance(KDTree node, Point p, int k) {
		float kDistance = Float.MAX_VALUE;
		PriorityQueue<PointEntry> pq = new PriorityQueue<PointEntry>(k,
				new PointEntryComparator());
		return kNNQuery(node, p, k, kDistance, pq, 0);
	}

	public static float exclusiveKDistance(KDTree node, Point p, int k) {
		float kDistance = Float.MAX_VALUE;
		PriorityQueue<PointEntry> pq = new PriorityQueue<PointEntry>(k,
				new PointEntryComparator());
		return kNNQuery(node, p, k + 1, kDistance, pq, 0);
	}

	private static float kNNQuery(KDTree node, Point p, int k, float kDistance,
			PriorityQueue<PointEntry> kNearestNeighbors, int level) {
		if (node.isLeaf) {
			for (Point o : node.getPoints()) {
				float distance = Point.euclideanDistance(o, p);
				if ((kDistance - distance) > 0.000001) {
					PointEntry pe = new PointEntry(o, distance);
					kNearestNeighbors.offer(pe);
					if (kNearestNeighbors.size() > k) {
						PointEntry[] tempRemoved = new PointEntry[kNearestNeighbors
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
					PointEntry pe = new PointEntry(o, distance);
					kNearestNeighbors.offer(pe);
				}
			}
			return kDistance;
		}
		int d = node.lowerBoundary.length;
		float pLevCoord = p.getP()[level];
		float splitCoord = node.left.upperBoundary[level];
		float distanceToSplit = (float) Math
				.abs/*sqrt*/(((pLevCoord - splitCoord) /* * (pLevCoord - splitCoord) */));
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
	private static float exclusiveKNNQuery(KDTree node, Point p, int k,
			float kDistance, PriorityQueue<PointEntry> kNearestNeighbors,
			int level) {
		if (node.isLeaf) {
			for (Point o : node.getPoints()) {
				float distance = Point.euclideanDistance(o, p);
				if (kDistance > distance) {
					PointEntry pe = new PointEntry(o, distance);
					if ((o.getID() != p.getID())
							|| (o.getCellID() != p.getCellID())) {
						kNearestNeighbors.offer(pe);
					}
					if (kNearestNeighbors.size() > k) {
						PointEntry[] tempRemoved = new PointEntry[kNearestNeighbors
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
					PointEntry pe = new PointEntry(o, distance);
					kNearestNeighbors.offer(pe);
				}
			}
			return kDistance;
		}
		int d = node.lowerBoundary.length;
		float pLevCoord = p.getP()[level];
		float splitCoord = node.left.upperBoundary[level];
		float distanceToSplit = (float) Math
				.sqrt(((pLevCoord - splitCoord) * (pLevCoord - splitCoord)));
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

	public static void findBoundaries(Point[] points, float[] lowerBoundary,
			float[] upperBoundary) {
		for (int i = 0; i < lowerBoundary.length; i++) {
			lowerBoundary[i] = Float.MAX_VALUE;
			upperBoundary[i] = Float.MIN_VALUE;
		}
		for (Point p : points) {
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

	public static boolean iskDistanceReady(KDTree tree, Point p, float kDistance) {
		float[] lowerBoundary = tree.lowerBoundary;
		float[] upperBoundary = tree.upperBoundary;
		float[] coords = p.getP();
		for (int i = 0; i < lowerBoundary.length; i++) {
			float distanceToBoundary = (float) Math
					.sqrt((lowerBoundary[i] - coords[i])
							* (lowerBoundary[i] - coords[i]));
			if ((kDistance - distanceToBoundary) > -0.000001) {
				return false;
			}
		}
		for (int i = 0; i < upperBoundary.length; i++) {
			float distanceToBoundary = (float) Math
					.sqrt((upperBoundary[i] - coords[i])
							* (upperBoundary[i] - coords[i]));
			if ((kDistance - distanceToBoundary) > -0.000001) {
				return false;
			}
		}
		return true;
	}

}
