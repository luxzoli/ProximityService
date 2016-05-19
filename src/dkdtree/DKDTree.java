package dkdtree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import kdtree.KDTree;
import kdtree.KDTreeTop;
import kdtree.Point;
import scala.Tuple2;
import dkdtree.BoundaryObject;

public class DKDTree implements Serializable {
	private KDTreeTop top;
	private JavaPairRDD<Integer, KDTree> subTrees;
	private float epsilon;
	private int sampleSize;
	private int numPartitions;
	private DistributePoints distributeF;
	private DKDTreePartitioner partitioner;

	public static JavaDStream<Point> streamKNNQuery(
			JavaDStream<Point> pointsStream, JavaRDD<Point> dataset, int k,
			int numPartitions, int sampleSize) {
		DKDTree dtree = new DKDTree(dataset, sampleSize, numPartitions);
		JavaPairDStream<Integer, Point> tKNNDStream = dtree.streamKNNQuery(
				pointsStream, k).persist(StorageLevel.MEMORY_ONLY());
		JavaDStream<Point> knnDStream = undoPairStream(tKNNDStream).persist(StorageLevel.MEMORY_ONLY());
		return knnDStream;
	}

	// TODO: decide whether the smaller epsilon should be considered
	public static JavaDStream<Point> streamEpsilonNeighborhoodKNNQuery(
			JavaDStream<Point> pointsStream, JavaRDD<Point> dataset, int k,
			float epsilon, int numPartitions, int sampleSize) {
		DKDTree dtree = new DKDTree(dataset, sampleSize, numPartitions, epsilon);
		JavaPairDStream<Integer, Point> tKNNDStream = dtree
				.streamEpsilonNeighborhoodKNNQuery(pointsStream, k, epsilon).persist(StorageLevel.MEMORY_ONLY());
		JavaDStream<Point> knnDStream = undoPairStream(tKNNDStream).persist(StorageLevel.MEMORY_ONLY());
		return knnDStream;
	}

	public static JavaRDD<Point> kNNQuery(JavaRDD<Point> points,
			JavaRDD<Point> dataset, int k, int numPartitions, int sampleSize) {
		DKDTree dtree = new DKDTree(dataset, sampleSize, numPartitions);
		JavaPairRDD<Integer, Point> tKNNs = dtree.kNNQuery(points, k);
		JavaRDD<Point> knns = undoPair(tKNNs);
		return knns;
	}

	// TODO: decide whether the smaller epsilon should be considered
	public static JavaRDD<Point> epsilonNeighborhoodKNNQuery(
			JavaRDD<Point> points, JavaRDD<Point> dataset, int k,
			float epsilon, int numPartitions, int sampleSize) {
		DKDTree dtree = new DKDTree(dataset, sampleSize, numPartitions, epsilon);
		JavaPairRDD<Integer, Point> teKNNs = dtree.epsilonNeighborhoodKNNQuery(
				points, k, epsilon);
		JavaRDD<Point> eKNNs = undoPair(teKNNs);
		return eKNNs;
	}

	private static JavaDStream<Point> undoPairStream(
			JavaPairDStream<Integer, Point> tKNNDStream) {
		Function<Tuple2<Integer, Point>, Point> f = new Function<Tuple2<Integer, Point>, Point>() {

			@Override
			public Point call(Tuple2<Integer, Point> arg0) throws Exception {
				return arg0._2;
			}

		};
		JavaDStream<Point> knnDStream = tKNNDStream.map(f);
		return knnDStream;
	}

	private static JavaRDD<Point> undoPair(JavaPairRDD<Integer, Point> pairs) {
		Function<Tuple2<Integer, Point>, Point> f = new Function<Tuple2<Integer, Point>, Point>() {

			@Override
			public Point call(Tuple2<Integer, Point> arg0) throws Exception {
				return arg0._2;
			}

		};
		JavaRDD<Point> points = pairs.map(f);
		return points;
	}

	public DKDTree(JavaRDD<Point> points, int sampleSize, int numPartitions) {
		this.sampleSize = sampleSize;
		this.numPartitions = numPartitions;
		JavaPairRDD<Integer, Point> pointsPairs = changeToPair(points);
		BoundaryObject boundary = calculateBoundaries(pointsPairs);

		KDTreeTop top = createKDTreeTop(sampleSize, numPartitions, pointsPairs,
				boundary);
		numPartitions = this.numPartitions = top.getNumPartitions();
		partitioner = new DKDTreePartitioner(numPartitions);

		this.distributeF = new DistributePoints(top);
		JavaPairRDD<Integer, KDTree> subTrees = createSubTrees(pointsPairs);
		this.subTrees = subTrees;
		this.top = top;
	}

	public DKDTree(JavaRDD<Point> points, int sampleSize, int numPartitions,
			float epsilon) {
		this.epsilon = epsilon;
		this.sampleSize = sampleSize;
		this.numPartitions = numPartitions;
		JavaPairRDD<Integer, Point> pointsPairs = changeToPair(points);
		BoundaryObject boundary = calculateBoundaries(pointsPairs);

		KDTreeTop top = createKDTreeTop(sampleSize, numPartitions, pointsPairs,
				boundary);
		numPartitions = this.numPartitions = top.getNumPartitions();
		this.partitioner = new DKDTreePartitioner(numPartitions);
		this.distributeF = new DistributePoints(top, epsilon);
		JavaPairRDD<Integer, KDTree> subTrees = createSubTrees(pointsPairs);
		this.subTrees = subTrees;
		this.top = top;
	}

	private JavaPairRDD<Integer, Point> changeToPair(JavaRDD<Point> points) {
		JavaPairRDD<Integer, Point> pointsPairs = points
				.mapToPair(new PairFunction<Point, Integer, Point>() {

					@Override
					public Tuple2<Integer, Point> call(Point arg0)
							throws Exception {
						// System.out.println(arg0);
						return new Tuple2<Integer, Point>(0, arg0);
					}

				});
		return pointsPairs;
	}

	public KDTreeTop createKDTreeTop(int sampleSize, int numPartitions,
			JavaPairRDD<Integer, Point> pointsPairs, BoundaryObject boundary) {
		List<Tuple2<Integer, Point>> pointsTL = pointsPairs.takeSample(false,
				sampleSize);
		Point[] pointsArr = new Point[pointsTL.size()];
		for (int i = 0; i < pointsArr.length; i++) {
			pointsArr[i] = pointsTL.get(i)._2;
		}
		int maxLeafSize = sampleSize / numPartitions;
		KDTreeTop top = new KDTreeTop(pointsArr, maxLeafSize, boundary.getMin()
				.getP(), boundary.getMax().getP());
		pointsArr = null;
		pointsTL = null;
		return top;
	}

	public BoundaryObject calculateBoundaries(
			JavaPairRDD<Integer, Point> pointsPairs) {
		Function<Point, BoundaryObject> createAcc = new Function<Point, BoundaryObject>() {
			public BoundaryObject call(Point p) {
				return new BoundaryObject(new Point(p), new Point(p));
			}
		};
		Function2<BoundaryObject, Point, BoundaryObject> addAndCount = new Function2<BoundaryObject, Point, BoundaryObject>() {
			public BoundaryObject call(BoundaryObject a, Point x) {
				float[] minA = a.getMin().getP();
				float[] maxA = a.getMax().getP();
				float[] p = x.getP();
				for (int d = 0; d < minA.length; d++) {
					minA[d] = p[d] < minA[d] ? p[d] : minA[d];
					maxA[d] = p[d] > maxA[d] ? p[d] : maxA[d];
				}
				a.setCount(a.getCount() + 1);
				return a;
			}
		};
		Function2<BoundaryObject, BoundaryObject, BoundaryObject> combine = new Function2<BoundaryObject, BoundaryObject, BoundaryObject>() {
			public BoundaryObject call(BoundaryObject a, BoundaryObject b) {
				float[] minA = a.getMin().getP();
				float[] maxA = a.getMax().getP();
				float[] minB = b.getMin().getP();
				float[] maxB = b.getMax().getP();
				for (int d = 0; d < minA.length; d++) {
					minA[d] = minB[d] < minA[d] ? minB[d] : minA[d];
					maxA[d] = maxB[d] > maxA[d] ? maxB[d] : maxA[d];
				}
				a.setCount(a.getCount() + b.getCount());
				return a;
			}
		};

		BoundaryObject boundary = pointsPairs
				.combineByKey(createAcc, addAndCount, combine)
				.reduceByKey(combine).first()._2();
		return boundary;
	}

	//TODO: kell-e count???
	private JavaPairRDD<Integer, KDTree> createSubTrees(
			JavaPairRDD<Integer, Point> pointsPairs) {
		JavaPairRDD<Integer, Point> partitionedPoints = pointsPairs
				.flatMapToPair(distributeF);

		JavaPairRDD<Integer, Iterable<Point>> partitions = partitionedPoints
				.partitionBy(partitioner).groupByKey();
		PairFunction<Tuple2<Integer, Iterable<Point>>, Integer, KDTree> createSubTrees = new PairFunction<Tuple2<Integer, Iterable<Point>>, Integer, KDTree>() {

			@Override
			public Tuple2<Integer, KDTree> call(
					Tuple2<Integer, Iterable<Point>> arg0) throws Exception {
				ArrayList<Point> pointsAL = new ArrayList<Point>();
				for (Point p : arg0._2) {
					pointsAL.add(p);
				}
				Point[] points = new Point[pointsAL.size()];
				int i = 0;
				for (Point point : pointsAL) {
					point.setCellID(arg0._1);
					//
					points[i] = point;
					i++;
				}
				pointsAL = null;
				KDTree tree = new KDTree(points, 10);
				return new Tuple2<Integer, KDTree>(arg0._1, tree);
			}

		};
		JavaPairRDD<Integer, KDTree> subTrees = partitions.mapToPair(
				createSubTrees).persist(StorageLevel.MEMORY_ONLY());
		subTrees.count();
		return subTrees;
	}

	public JavaPairDStream<Integer, Point> streamKNNQuery(
			JavaDStream<Point> points, int k) {

		JavaPairDStream<Integer, Point> pointsStream = changeToPairStream(points);
		DistributePointsToSingle distributeF = new DistributePointsToSingle(top);
		JavaPairDStream<Integer, Tuple2<Iterable<Point>, KDTree>> groupsDStream = pointsStream
				.flatMapToPair(distributeF).transformToPair(transformFunc)
				.mapValues(collectOnly);
		KNN1 knn1 = new KNN1(k);
		JavaPairDStream<Integer, Point> knn1DStream = groupsDStream
				.flatMapToPair(knn1);
		JavaPairDStream<Integer, Point> notReadyDStream = knn1DStream
				.filter(filterNotReady);
		JavaPairDStream<Integer, Point> readyDStream = knn1DStream
				.filter(filterReady);
		KNN2 knn2 = new KNN2(k);
		KNN2FlatMap notReadyMap = new KNN2FlatMap(this.top);
		JavaPairDStream<Integer, Point> knn2DStream = notReadyDStream
				.flatMapToPair(notReadyMap).transformToPair(transformFunc)
				.mapValues(collectOnly).flatMapToPair(knn2);
		JavaPairDStream<Integer, Point> knn3TempDStream = notReadyDStream
				.union(knn2DStream);
		KNN3 knn3 = new KNN3(k);
		JavaPairDStream<Integer, Point> knn3DStream = knn3TempDStream
				.mapToPair(IDF).groupByKey().mapToPair(knn3);

		JavaPairDStream<Integer, Point> knnDStream = readyDStream
				.union(knn3DStream);
		return knnDStream;
	}

	public JavaPairDStream<Integer, Point> streamEpsilonNeighborhoodKNNQuery(
			JavaDStream<Point> points, int k, float epsilon) {

		JavaPairDStream<Integer, Point> pointsStream = changeToPairStream(points);
		DistributePointsToSingle distributeF = new DistributePointsToSingle(top);
		JavaPairDStream<Integer, Tuple2<Iterable<Point>, KDTree>> groupsDStream = pointsStream
				.flatMapToPair(distributeF).transformToPair(transformFunc)
				.mapValues(collectOnly);
		EpsilonNeighborhoodKNN eknn = new EpsilonNeighborhoodKNN(k, epsilon);
		JavaPairDStream<Integer, Point> knnDStream = groupsDStream
				.flatMapToPair(eknn);
		return knnDStream;
	}

	public JavaPairRDD<Integer, Point> kNNQuery(JavaRDD<Point> points, int k) {

		JavaPairRDD<Integer, Point> pointPairs = changeToPair(points);
		DistributePointsToSingle distributeF = new DistributePointsToSingle(top);
		JavaPairRDD<Integer, Tuple2<Iterable<Point>, KDTree>> groups = pointPairs
				.flatMapToPair(distributeF).partitionBy(partitioner)
				.cogroup(subTrees).mapValues(collectOnly);
		KNN1 knn1 = new KNN1(k);
		JavaPairRDD<Integer, Point> knn1R = groups.flatMapToPair(knn1);
		JavaPairRDD<Integer, Point> notReadyR = knn1R.filter(filterNotReady);
		JavaPairRDD<Integer, Point> readyR = knn1R.filter(filterReady);
		KNN2 knn2 = new KNN2(k);
		KNN2FlatMap notReadyMap = new KNN2FlatMap(this.top);
		JavaPairRDD<Integer, Point> knn2R = notReadyR
				.flatMapToPair(notReadyMap).partitionBy(partitioner)
				.cogroup(subTrees).mapValues(collectOnly).flatMapToPair(knn2);
		JavaPairRDD<Integer, Point> knn3TempR = notReadyR.union(knn2R);
		KNN3 knn3 = new KNN3(k);
		JavaPairRDD<Integer, Point> knn3R = knn3TempR.mapToPair(IDF)
				.groupByKey().mapToPair(knn3);

		JavaPairRDD<Integer, Point> knns = readyR.union(knn3R);
		return knns;
	}

	public JavaPairRDD<Integer, Point> epsilonNeighborhoodKNNQuery(
			JavaRDD<Point> points, int k, float epsilon) {

		JavaPairRDD<Integer, Point> pointsStream = changeToPair(points);
		DistributePointsToSingle distributeF = new DistributePointsToSingle(top);
		JavaPairRDD<Integer, Tuple2<Iterable<Point>, KDTree>> groupsDStream = pointsStream
				.flatMapToPair(distributeF).partitionBy(partitioner)
				.cogroup(subTrees).mapValues(collectOnly);
		EpsilonNeighborhoodKNN eknn = new EpsilonNeighborhoodKNN(k, epsilon);
		JavaPairRDD<Integer, Point> eKNNs = groupsDStream.flatMapToPair(eknn);
		return eKNNs;
	}

	private JavaPairDStream<Integer, Point> changeToPairStream(
			JavaDStream<Point> points) {
		JavaPairDStream<Integer, Point> pointsStream = points
				.mapToPair(new PairFunction<Point, Integer, Point>() {

					@Override
					public Tuple2<Integer, Point> call(Point arg0)
							throws Exception {
						return new Tuple2<Integer, Point>(0, arg0);
					}

				});
		return pointsStream;
	}

	Function<JavaPairRDD<Integer, Point>, JavaPairRDD<Integer, Tuple2<Iterable<Point>, Iterable<KDTree>>>> transformFunc = new Function<JavaPairRDD<Integer, Point>, JavaPairRDD<Integer, Tuple2<Iterable<Point>, Iterable<KDTree>>>>() {

		@Override
		public JavaPairRDD<Integer, Tuple2<Iterable<Point>, Iterable<KDTree>>> call(
				JavaPairRDD<Integer, Point> arg0) throws Exception {
			return arg0.partitionBy(partitioner).cogroup(subTrees);
		}

	};
	Function<Tuple2<Iterable<Point>, Iterable<KDTree>>, Tuple2<Iterable<Point>, KDTree>> collectOnly = new Function<Tuple2<Iterable<Point>, Iterable<KDTree>>, Tuple2<Iterable<Point>, KDTree>>() {

		@Override
		public Tuple2<Iterable<Point>, KDTree> call(
				Tuple2<Iterable<Point>, Iterable<KDTree>> arg0)
				throws Exception {
			// TODO Auto-generated method stub
			return new Tuple2<Iterable<Point>, KDTree>(arg0._1, arg0._2
					.iterator().next());
		}

	};

	Function<Tuple2<Integer, Point>, Boolean> filterNotReady = new Function<Tuple2<Integer, Point>, Boolean>() {

		@Override
		public Boolean call(Tuple2<Integer, Point> arg0) throws Exception {
			return !arg0._2.isReady();
		}

	};
	PairFunction<Tuple2<Integer, Point>, Long, Point> IDF = new PairFunction<Tuple2<Integer, Point>, Long, Point>() {
		@Override
		public Tuple2<Long, Point> call(Tuple2<Integer, Point> arg0)
				throws Exception {
			Point p = arg0._2;
			return new Tuple2<Long, Point>(p.getID(), p);
		}
	};

	Function<Tuple2<Integer, Point>, Boolean> filterReady = new Function<Tuple2<Integer, Point>, Boolean>() {

		@Override
		public Boolean call(Tuple2<Integer, Point> arg0) throws Exception {

			return arg0._2.isReady();
		}

	};

	public KDTreeTop getTop() {
		return top;
	}

	public JavaPairRDD<Integer, KDTree> getSubTrees() {
		return subTrees;
	}

}
