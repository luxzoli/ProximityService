package geo.kdtree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;


public class GeoKDTreeTop implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5741300461453704158L;
	private int maxLeafSize;
	private int count = 0;
	private GeoPoint[] points;
	private boolean isLeaf;
	private boolean isRoot = false;
	private GeoKDTreeTop left;
	private GeoKDTreeTop right;
	private int leafID;
	private float[] lowerBoundary;
	private float[] upperBoundary;
	private static ArrayList<GeoKDTreeTop> leaves;
	private static final float R = 6371000; // Radius of the earth in meters
	static {
		leaves = new ArrayList<GeoKDTreeTop>();
	}

	private GeoKDTreeTop() {

	}

	public GeoKDTreeTop(GeoPoint[] points, int maxLeafSize, float[] lowerBoundary,
			float[] upperBoundary) {
		GeoKDTreeTop.leaves.clear();
		this.isRoot = true;
		this.setCount(points.length);
		this.maxLeafSize = maxLeafSize;
		// this.setID(0);
		this.lowerBoundary = lowerBoundary;
		this.upperBoundary = upperBoundary;
		if (points.length <= this.maxLeafSize
				|| GeoKDTreeTop.sameBoundaries(lowerBoundary, upperBoundary)) {
			// this.setPoints(points);
			isLeaf = true;
			leaves.add(this);
			this.leafID = leaves.size() - 1;
			return;
		}
		int actualDimension = 0;
		// other possibility is median of medians
		GeoPointComparator pc = new GeoPointComparator(actualDimension);
		Arrays.sort(points, pc);
		int half = points.length / 2 + points.length % 2 - 1;
		// int leftSize = half;
		while (points[half + 1] == points[half]) {
			half++;
		}
		int leftFromIndex = 0;
		int leftToIndex = half + 1;
		int rightFromIndex = half + 1;
		int rightToIndex = points.length;
		GeoPoint median = calculateMedian(points, half);
		/*
		 * Point[] leftArr = new Point[leftSize]; Point[] rightArr = new
		 * Point[points.length - leftSize]; for (int i = 0; i < leftSize; i++) {
		 * leftArr[i] = points[i]; } for (int i = leftSize; i < points.length;
		 * i++) { rightArr[i - leftSize] = points[i]; }
		 */
		float[] leftUpperBoundary = new float[upperBoundary.length];
		float[] rightLowerBoundary = new float[lowerBoundary.length];
		calculateBoundaries(lowerBoundary, upperBoundary, actualDimension,
				median, leftUpperBoundary, rightLowerBoundary);
		int d = points[0].getP().length;
		left = new GeoKDTreeTop(points, maxLeafSize, (actualDimension + 1) % d,
				lowerBoundary, leftUpperBoundary, leftFromIndex, leftToIndex);
		right = new GeoKDTreeTop(points, maxLeafSize, (actualDimension + 1) % d,
				rightLowerBoundary, upperBoundary, rightFromIndex, rightToIndex);
		isLeaf = false;
		this.leafID = -1;
	}

	private GeoKDTreeTop(GeoPoint[] points, int maxLeafSize, int actualDimension,
			float[] lowerBoundary, float[] upperBoundary, int fromIndex,
			int toIndex) {
		this.setCount(points.length);
		this.maxLeafSize = maxLeafSize;
		// this.setID(0);
		this.lowerBoundary = lowerBoundary;
		this.upperBoundary = upperBoundary;
		if ((toIndex - fromIndex) <= this.maxLeafSize
				|| GeoKDTreeTop.sameBoundaries(lowerBoundary, upperBoundary)) {
			// this.setPoints(points);
			isLeaf = true;
			leaves.add(this);
			this.leafID = leaves.size() - 1;
			return;
		}
		// other possibility is median of medians
		GeoPointComparator pc = new GeoPointComparator(actualDimension);
		Arrays.sort(points, fromIndex, toIndex, pc);
		int half = (toIndex - fromIndex) / 2 + (toIndex - fromIndex) % 2
				+ fromIndex - 1;
		// int leftSize = half;
		while (points[half + 1] == points[half]) {
			half++;
		}
		int leftFromIndex = fromIndex;
		int leftToIndex = half + 1;
		int rightFromIndex = half + 1;
		int rightToIndex = toIndex;
		GeoPoint median = calculateMedian(points, half);
		/*
		 * Point[] leftArr = new Point[leftSize]; Point[] rightArr = new
		 * Point[points.length - leftSize]; for (int i = 0; i < leftSize; i++) {
		 * leftArr[i] = points[i]; } for (int i = leftSize; i < points.length;
		 * i++) { rightArr[i - leftSize] = points[i]; }
		 */
		float[] leftUpperBoundary = new float[2];
		float[] rightLowerBoundary = new float[2];
		calculateBoundaries(lowerBoundary, upperBoundary, actualDimension,
				median, leftUpperBoundary, rightLowerBoundary);
		int d = 2;
		left = new GeoKDTreeTop(points, maxLeafSize, (actualDimension + 1) % d,
				lowerBoundary, leftUpperBoundary, leftFromIndex, leftToIndex);
		right = new GeoKDTreeTop(points, maxLeafSize, (actualDimension + 1) % d,
				rightLowerBoundary, upperBoundary, rightFromIndex, rightToIndex);
		isLeaf = false;
		this.leafID = -1;
	}

	public static void rangeSearch(GeoKDTreeTop node, float[] lowerBoundary,
			float[] upperBoundary, ArrayList<GeoKDTreeTop> nodesInRange) {
		rangeSearch(node, lowerBoundary, upperBoundary, nodesInRange, 0);
	}

	private static void rangeSearch(GeoKDTreeTop node, float[] lowerBoundary,
			float[] upperBoundary, ArrayList<GeoKDTreeTop> nodesInRange, int level) {
		int d = 2;
		if (upperBoundary[level] < node.lowerBoundary[level]) {
			return;
		} else if (lowerBoundary[level] > node.upperBoundary[level]) {
			return;
		}

		if (node.isLeaf) {
			nodesInRange.add(node);
		} else {
			rangeSearch(node.left, lowerBoundary, upperBoundary, nodesInRange,
					(level + 1) % d);
			rangeSearch(node.right, lowerBoundary, upperBoundary, nodesInRange,
					(level + 1) % d);
		}
	}

	public static void epsilonNeighborhood(GeoKDTreeTop node, GeoPoint p, float epsilon,
			ArrayList<GeoKDTreeTop> neighbors) {
		int d = p.getD();
		float[] coords = p.getP();
		float[] upperBoundary = new float[d];
		float[] lowerBoundary = new float[d];
		for (int i = 0; i < d; i++) {
			upperBoundary[i] = coords[i] + epsilon;
			lowerBoundary[i] = coords[i] - epsilon;
		}
		rangeSearch(node, lowerBoundary, upperBoundary, neighbors);

	}

	private void calculateBoundaries(float[] lowerBoundary,
			float[] upperBoundary, int actualDimension, GeoPoint median,
			float[] leftUpperBoundary, float[] rightLowerBoundary) {
		for (int i = 0; i < 2; i++) {
			if (i == actualDimension) {
				leftUpperBoundary[i] = rightLowerBoundary[i] = median.getP()[i];
			} else {
				leftUpperBoundary[i] = upperBoundary[i];
				rightLowerBoundary[i] = lowerBoundary[i];
			}
		}
	}

	//TODO: epsilon: km or lat/lon???
	public boolean isInside(GeoPoint point, float epsilon) {
		epsilon = (float) (epsilon / R * 180 / Math.PI); 
		float[] coords = point.getP();
		for (int i = 0; i < 2; i++) {
			if ((coords[i] >= (this.lowerBoundary[i] - epsilon))
					&& (coords[i] <= (this.upperBoundary[i] + epsilon))) {
				continue;
			} else {
				return false;
			}
		}
		return true;
	}

	//TODO: epsilon: km or lat/lon???
	public static boolean isInside(GeoKDTreeTop grid, GeoPoint point, float epsilon) {
		epsilon = (float) (epsilon / R * 180 / Math.PI); 
		float[] coords = point.getP();
		for (int i = 0; i < 2; i++) {
			if ((coords[i] >= (grid.lowerBoundary[i] - epsilon))
					&& (coords[i] <= (grid.upperBoundary[i] + epsilon))) {
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

	public static boolean isInside(GeoKDTreeTop grid, GeoPoint point) {
		float[] coords = point.getP();
		for (int i = 0; i < grid.lowerBoundary.length; i++) {
			if ((coords[i] >= grid.lowerBoundary[i])
					&& (coords[i] <= grid.upperBoundary[i])) {
				continue;
			} else {
				return false;
			}
		}
		return true;
	}

	public ArrayList<GeoKDTreeTop> getMatchingGrids(GeoPoint point, float epsilon) {
		ArrayList<GeoKDTreeTop> matching = new ArrayList<GeoKDTreeTop>();
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

	public static void getMatchingGrids(GeoKDTreeTop grid, GeoPoint point, float epsilon,
			ArrayList<GeoKDTreeTop> matching) {
		if (grid.isLeaf) {
			if (GeoKDTreeTop.isInside(grid, point, epsilon)) {
				matching.add(grid);
			}
		} else {
			if (GeoKDTreeTop.isInside(grid.left, point, epsilon)) {
				getMatchingGrids(grid.left, point, epsilon, matching);
			}
			if (GeoKDTreeTop.isInside(grid.right, point, epsilon)) {
				getMatchingGrids(grid.right, point, epsilon, matching);
			}
		}
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

	public static GeoKDTreeTop readFromString(String gridAsString) {
		// System.out.println(gridAsString);
		GeoKDTreeTop grid = new GeoKDTreeTop();
		if (gridAsString.startsWith("{root")) {
			GeoKDTreeTop.leaves.clear();
			grid.isLeaf = false;
			grid.isRoot = true;
			int firstComma = gridAsString.indexOf(",");
			int secondComma = gridAsString.indexOf(",", firstComma + 1);
			int maxLeafSize = Integer.parseInt(gridAsString.substring(
					firstComma + 1, secondComma));
			grid.maxLeafSize = maxLeafSize;
			int fbc = GeoKDTreeTop.findClosure(gridAsString.substring(secondComma + 1),
					'[', ']') + secondComma + 1;
			String lowerBoundaryString = gridAsString.substring(
					secondComma + 1, fbc + 1);
			float[] lowerBoundary = GeoKDTreeTop.parseBoundary(lowerBoundaryString);
			int sbc = GeoKDTreeTop.findClosure(gridAsString.substring(fbc + 2), '[',
					']') + fbc + 2;
			grid.lowerBoundary = lowerBoundary;
			String upperBoundaryString = gridAsString.substring(fbc + 2,
					sbc + 1);
			float[] upperBoundary = GeoKDTreeTop.parseBoundary(upperBoundaryString);
			grid.upperBoundary = upperBoundary;
			int lcc = GeoKDTreeTop.findClosure(gridAsString.substring(sbc + 2), '{',
					'}') + sbc + 2;
			int rcc = GeoKDTreeTop.findClosure(gridAsString.substring(lcc + 2), '{',
					'}') + lcc + 2;
			String leftChildString = gridAsString.substring(sbc + 2, lcc + 1);
			if (leftChildString.length() == 0) {
				grid.isLeaf = true;
			}
			if (leftChildString.length() > 0) {
				GeoKDTreeTop left = readFromString(leftChildString);
				grid.left = left;
			}
			String rightChildString = gridAsString.substring(lcc + 2, rcc + 1);
			if (rightChildString.length() > 0) {
				GeoKDTreeTop right = readFromString(rightChildString);
				grid.right = right;
			}
			grid.leafID = -1;
			// readFromString(gridAsString);
		} else if (gridAsString.startsWith("{inner_node")) {
			grid.isLeaf = false;
			int firstComma = gridAsString.indexOf(",");
			int fbc = GeoKDTreeTop.findClosure(gridAsString.substring(firstComma + 1),
					'[', ']') + firstComma + 1;
			String lowerBoundaryString = gridAsString.substring(firstComma + 1,
					fbc + 1);
			float[] lowerBoundary = GeoKDTreeTop.parseBoundary(lowerBoundaryString);
			int sbc = GeoKDTreeTop.findClosure(gridAsString.substring(fbc + 2), '[',
					']') + fbc + 2;
			grid.lowerBoundary = lowerBoundary;
			String upperBoundaryString = gridAsString.substring(fbc + 2,
					sbc + 1);
			float[] upperBoundary = GeoKDTreeTop.parseBoundary(upperBoundaryString);
			grid.upperBoundary = upperBoundary;
			int lcc = GeoKDTreeTop.findClosure(gridAsString.substring(sbc + 2), '{',
					'}') + sbc + 2;
			int rcc = GeoKDTreeTop.findClosure(gridAsString.substring(lcc + 2), '{',
					'}') + lcc + 2;
			String leftChildString = gridAsString.substring(sbc + 2, lcc + 1);
			GeoKDTreeTop left = readFromString(leftChildString);
			grid.left = left;
			String rightChildString = gridAsString.substring(lcc + 2, rcc + 1);
			GeoKDTreeTop right = readFromString(rightChildString);
			grid.right = right;
			grid.leafID = -1;
		} else if (gridAsString.startsWith("{leaf")) {
			int firstComma = gridAsString.indexOf(",");
			int fbc = GeoKDTreeTop.findClosure(gridAsString.substring(firstComma + 1),
					'[', ']') + firstComma + 1;
			String lowerBoundaryString = gridAsString.substring(firstComma + 1,
					fbc + 1);
			float[] lowerBoundary = GeoKDTreeTop.parseBoundary(lowerBoundaryString);
			int sbc = GeoKDTreeTop.findClosure(gridAsString.substring(fbc + 2), '[',
					']') + fbc + 1;
			grid.lowerBoundary = lowerBoundary;
			String upperBoundaryString = gridAsString.substring(fbc + 2,
					sbc + 1);
			float[] upperBoundary = GeoKDTreeTop.parseBoundary(upperBoundaryString);
			grid.upperBoundary = upperBoundary;
			grid.isLeaf = true;
			GeoKDTreeTop.leaves.add(grid);
			grid.leafID = leaves.size();
		}
		return grid;
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

	private void setCount(int count) {
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
		int d = points[0].getP().length;
		float[] medianArr = new float[d];
		float[] medianArrLower = points[half].getP();
		float[] medianArrUpper = points[half + ((half + 1) % 2)].getP();
		for (int i = 0; i < d; i++) {
			medianArr[i] = (medianArrLower[i] + medianArrUpper[i]) / 2;
		}
		return new GeoPoint(medianArr);
	}
	
	public int getNumPartitions() {
		return GeoKDTreeTop.leaves.size();
	}

}
