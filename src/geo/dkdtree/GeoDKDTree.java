package geo.dkdtree;

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

import scala.Tuple2;
import geo.dkdtree.BoundaryObject;
import geo.kdtree.GeoKDTree;
import geo.kdtree.GeoKDTreeTop;
import geo.kdtree.GeoPoint;

public class GeoDKDTree implements Serializable {
	
	private static final long serialVersionUID = 7621961808913115021L;
	private GeoKDTreeTop top;
	private JavaPairRDD<Integer, GeoKDTree> subTrees;
	private float epsilon;
	private int sampleSize;
	private int numPartitions;
	private DistributePoints distributeF;
	private GeoDKDTreePartitioner partitioner;

	public static JavaDStream<GeoPoint> streamKNNQuery(
			JavaDStream<GeoPoint> pointsStream, JavaRDD<GeoPoint> dataset, int k,
			int numPartitions, int sampleSize) {
		GeoDKDTree dtree = new GeoDKDTree(dataset, sampleSize, numPartitions);
		JavaPairDStream<Integer, GeoPoint> tKNNDStream = dtree.streamKNNQuery(
				pointsStream, k);
		JavaDStream<GeoPoint> knnDStream = undoPairStream(tKNNDStream);
		return knnDStream;
	}

	// TODO: decide whether the smaller epsilon should be considered
	public static JavaDStream<GeoPoint> streamEpsilonNeighborhoodKNNQuery(
			JavaDStream<GeoPoint> pointsStream, JavaRDD<GeoPoint> dataset, int k,
			float epsilon, int numPartitions, int sampleSize) {
		GeoDKDTree dtree = new GeoDKDTree(dataset, sampleSize, numPartitions, epsilon);
		JavaPairDStream<Integer, GeoPoint> tKNNDStream = dtree
				.streamEpsilonNeighborhoodKNNQuery(pointsStream, k, epsilon);
		JavaDStream<GeoPoint> knnDStream = undoPairStream(tKNNDStream);
		return knnDStream;
	}

	public static JavaRDD<GeoPoint> kNNQuery(JavaRDD<GeoPoint> points,
			JavaRDD<GeoPoint> dataset, int k, int numPartitions, int sampleSize) {
		GeoDKDTree dtree = new GeoDKDTree(dataset, sampleSize, numPartitions);
		JavaPairRDD<Integer, GeoPoint> tKNNs = dtree.kNNQuery(points, k);
		JavaRDD<GeoPoint> knns = undoPair(tKNNs);
		return knns;
	}

	// TODO: decide whether the smaller epsilon should be considered
	public static JavaRDD<GeoPoint> epsilonNeighborhoodKNNQuery(
			JavaRDD<GeoPoint> points, JavaRDD<GeoPoint> dataset, int k,
			float epsilon, int numPartitions, int sampleSize) {
		GeoDKDTree dtree = new GeoDKDTree(dataset, sampleSize, numPartitions, epsilon);
		JavaPairRDD<Integer, GeoPoint> teKNNs = dtree.epsilonNeighborhoodKNNQuery(
				points, k, epsilon);
		JavaRDD<GeoPoint> eKNNs = undoPair(teKNNs);
		return eKNNs;
	}

	private static JavaDStream<GeoPoint> undoPairStream(
			JavaPairDStream<Integer, GeoPoint> tKNNDStream) {
		Function<Tuple2<Integer, GeoPoint>, GeoPoint> f = new Function<Tuple2<Integer, GeoPoint>, GeoPoint>() {

			@Override
			public GeoPoint call(Tuple2<Integer, GeoPoint> arg0) throws Exception {
				return arg0._2;
			}

		};
		JavaDStream<GeoPoint> knnDStream = tKNNDStream.map(f);
		return knnDStream;
	}

	private static JavaRDD<GeoPoint> undoPair(JavaPairRDD<Integer, GeoPoint> pairs) {
		@SuppressWarnings("serial")
		Function<Tuple2<Integer, GeoPoint>, GeoPoint> f = new Function<Tuple2<Integer, GeoPoint>, GeoPoint>() {

			@Override
			public GeoPoint call(Tuple2<Integer, GeoPoint> arg0) throws Exception {
				return arg0._2;
			}

		};
		JavaRDD<GeoPoint> points = pairs.map(f);
		return points;
	}

	public GeoDKDTree(JavaRDD<GeoPoint> points, int sampleSize, int numPartitions) {
		this.sampleSize = sampleSize;
		this.numPartitions = numPartitions;
		JavaPairRDD<Integer, GeoPoint> pointsPairs = changeToPair(points);
		BoundaryObject boundary = calculateBoundaries(pointsPairs);
		System.out.println(boundary);

		GeoKDTreeTop top = createKDTreeTop(sampleSize, numPartitions, pointsPairs,
				boundary);
		numPartitions = this.numPartitions = top.getNumPartitions();
		partitioner = new GeoDKDTreePartitioner(numPartitions);

		this.distributeF = new DistributePoints(top);
		JavaPairRDD<Integer, GeoKDTree> subTrees = createSubTrees(pointsPairs);
		this.subTrees = subTrees;
		this.top = top;
	}

	public GeoDKDTree(JavaRDD<GeoPoint> points, int sampleSize, int numPartitions,
			float epsilon) {
		this.epsilon = epsilon;
		this.sampleSize = sampleSize;
		this.numPartitions = numPartitions;
		JavaPairRDD<Integer, GeoPoint> pointsPairs = changeToPair(points);
		BoundaryObject boundary = calculateBoundaries(pointsPairs);
		System.out.println(boundary);

		GeoKDTreeTop top = createKDTreeTop(sampleSize, numPartitions, pointsPairs,
				boundary);
		numPartitions = this.numPartitions = top.getNumPartitions();
		this.partitioner = new GeoDKDTreePartitioner(numPartitions);
		this.distributeF = new DistributePoints(top, epsilon);
		JavaPairRDD<Integer, GeoKDTree> subTrees = createSubTrees(pointsPairs);
		this.subTrees = subTrees;
		this.top = top;
	}

	private JavaPairRDD<Integer, GeoPoint> changeToPair(JavaRDD<GeoPoint> points) {
		@SuppressWarnings("serial")
		JavaPairRDD<Integer, GeoPoint> pointsPairs = points
				.mapToPair(new PairFunction<GeoPoint, Integer, GeoPoint>() {

					@Override
					public Tuple2<Integer, GeoPoint> call(GeoPoint arg0)
							throws Exception {
						// System.out.println(arg0);
						return new Tuple2<Integer, GeoPoint>(0, arg0);
					}

				});
		return pointsPairs;
	}

	public GeoKDTreeTop createKDTreeTop(int sampleSize, int numPartitions,
			JavaPairRDD<Integer, GeoPoint> pointsPairs, BoundaryObject boundary) {
		List<Tuple2<Integer, GeoPoint>> pointsTL = pointsPairs.takeSample(false,
				sampleSize);
		GeoPoint[] pointsArr = new GeoPoint[pointsTL.size()];
		for (int i = 0; i < pointsArr.length; i++) {
			pointsArr[i] = pointsTL.get(i)._2;
		}
		int maxLeafSize = sampleSize / numPartitions;
		GeoKDTreeTop top = new GeoKDTreeTop(pointsArr, maxLeafSize, boundary.getMin()
				.getP(), boundary.getMax().getP());
		pointsArr = null;
		pointsTL = null;
		return top;
	}

	public BoundaryObject calculateBoundaries(
			JavaPairRDD<Integer, GeoPoint> pointsPairs) {
		@SuppressWarnings("serial")
		Function<GeoPoint, BoundaryObject> createAcc = new Function<GeoPoint, BoundaryObject>() {
			public BoundaryObject call(GeoPoint p) {
				return new BoundaryObject(new GeoPoint(p), new GeoPoint(p));
			}
		};
		@SuppressWarnings("serial")
		Function2<BoundaryObject, GeoPoint, BoundaryObject> addAndCount = new Function2<BoundaryObject, GeoPoint, BoundaryObject>() {
			public BoundaryObject call(BoundaryObject a, GeoPoint x) {
				float minALat = a.getMin().getLat();
				float minALon = a.getMin().getLon();
				float maxALat = a.getMax().getLat();
				float maxALon = a.getMax().getLon();
				float lat = x.getLat();
				float lon = x.getLon();
				minALat = lat < minALat ? lat : minALat;
				maxALat = lat > maxALat ? lat : maxALat;
				minALon = lon < minALon ? lon : minALon;
				maxALon = lon > maxALon ? lon : maxALon;
				a.getMin().setLat(minALat);
				a.getMin().setLon(minALon);
				a.getMax().setLat(maxALat);
				a.getMax().setLon(maxALon);
				a.setCount(a.getCount() + 1);
				return a;
			}
		};
		@SuppressWarnings("serial")
		Function2<BoundaryObject, BoundaryObject, BoundaryObject> combine = new Function2<BoundaryObject, BoundaryObject, BoundaryObject>() {
			public BoundaryObject call(BoundaryObject a, BoundaryObject b) {
				float minALat = a.getMin().getLat();
				float minALon = a.getMin().getLon();
				float maxALat = a.getMax().getLat();
				float maxALon = a.getMax().getLon();
				float minBLat = b.getMin().getLat();
				float minBLon = b.getMin().getLon();
				float maxBLat = b.getMax().getLat();
				float maxBLon = b.getMax().getLon();
				minALat = minBLat < minALat ? minBLat : minALat;
				maxALat = maxBLat > maxALat ? maxBLat : maxALat;
				minALon = minBLon < minALon ? minBLon : minALon;
				maxALon = maxBLon > maxALon ? maxBLon : maxALon;
				a.getMin().setLat(minALat);
				a.getMin().setLon(minALon);
				a.getMax().setLat(maxALat);
				a.getMax().setLon(maxALon);
				a.setCount(a.getCount() + b.getCount());
				return a;
			}
		};

		BoundaryObject boundary = pointsPairs
				.combineByKey(createAcc, addAndCount, combine)
				.reduceByKey(combine).first()._2();
		return boundary;
	}

	// TODO: kell-e count???
	private JavaPairRDD<Integer, GeoKDTree> createSubTrees(
			JavaPairRDD<Integer, GeoPoint> pointsPairs) {
		JavaPairRDD<Integer, GeoPoint> partitionedPoints = pointsPairs
				.flatMapToPair(distributeF);

		JavaPairRDD<Integer, Iterable<GeoPoint>> partitions = partitionedPoints
				.partitionBy(partitioner).groupByKey();
		@SuppressWarnings("serial")
		PairFunction<Tuple2<Integer, Iterable<GeoPoint>>, Integer, GeoKDTree> createSubTrees = new PairFunction<Tuple2<Integer, Iterable<GeoPoint>>, Integer, GeoKDTree>() {

			@Override
			public Tuple2<Integer, GeoKDTree> call(
					Tuple2<Integer, Iterable<GeoPoint>> arg0) throws Exception {
				ArrayList<GeoPoint> pointsAL = new ArrayList<GeoPoint>();
				for (GeoPoint p : arg0._2) {
					pointsAL.add(p);
				}
				GeoPoint[] points = new GeoPoint[pointsAL.size()];
				int i = 0;
				for (GeoPoint point : pointsAL) {
					point.setCellID(arg0._1);
					//
					points[i] = point;
					i++;
				}
				pointsAL = null;
				GeoKDTree tree = new GeoKDTree(points, 10);
				return new Tuple2<Integer, GeoKDTree>(arg0._1, tree);
			}

		};
		JavaPairRDD<Integer, GeoKDTree> subTrees = partitions.mapToPair(
				createSubTrees).persist(StorageLevel.MEMORY_ONLY());
		subTrees.count();
		return subTrees;
	}

	public JavaPairDStream<Integer, GeoPoint> streamKNNQuery(
			JavaDStream<GeoPoint> points, int k) {

		JavaPairDStream<Integer, GeoPoint> pointsStream = changeToPairStream(points);
		DistributePointsToSingle distributeF = new DistributePointsToSingle(top);
		JavaPairDStream<Integer, Tuple2<Iterable<GeoPoint>, GeoKDTree>> groupsDStream = pointsStream
				.flatMapToPair(distributeF).transformToPair(transformFunc)
				.mapValues(collectOnly);
		KNN1 knn1 = new KNN1(k);
		JavaPairDStream<Integer, GeoPoint> knn1DStream = groupsDStream
				.flatMapToPair(knn1);
		JavaPairDStream<Integer, GeoPoint> notReadyDStream = knn1DStream
				.filter(filterNotReady);
		JavaPairDStream<Integer, GeoPoint> readyDStream = knn1DStream
				.filter(filterReady);
		KNN2 knn2 = new KNN2(k);
		KNN2FlatMap notReadyMap = new KNN2FlatMap(this.top);
		JavaPairDStream<Integer, GeoPoint> knn2DStream = notReadyDStream
				.flatMapToPair(notReadyMap).transformToPair(transformFunc)
				.mapValues(collectOnly).flatMapToPair(knn2);
		JavaPairDStream<Integer, GeoPoint> knn3TempDStream = notReadyDStream
				.union(knn2DStream);
		KNN3 knn3 = new KNN3(k);
		JavaPairDStream<Integer, GeoPoint> knn3DStream = knn3TempDStream
				.mapToPair(IDF).groupByKey().mapToPair(knn3);

		JavaPairDStream<Integer, GeoPoint> knnDStream = readyDStream
				.union(knn3DStream);
		return knnDStream;
	}

	public JavaPairDStream<Integer, GeoPoint> streamEpsilonNeighborhoodKNNQuery(
			JavaDStream<GeoPoint> points, int k, float epsilon) {

		JavaPairDStream<Integer, GeoPoint> pointsStream = changeToPairStream(points);
		DistributePointsToSingle distributeF = new DistributePointsToSingle(top);
		JavaPairDStream<Integer, Tuple2<Iterable<GeoPoint>, GeoKDTree>> groupsDStream = pointsStream
				.flatMapToPair(distributeF).transformToPair(transformFunc)
				.mapValues(collectOnly);
		EpsilonNeighborhoodKNN eknn = new EpsilonNeighborhoodKNN(k, epsilon);
		JavaPairDStream<Integer, GeoPoint> knnDStream = groupsDStream
				.flatMapToPair(eknn);
		return knnDStream;
	}

	public JavaPairRDD<Integer, GeoPoint> kNNQuery(JavaRDD<GeoPoint> points, int k) {

		JavaPairRDD<Integer, GeoPoint> pointPairs = changeToPair(points);
		DistributePointsToSingle distributeF = new DistributePointsToSingle(top);
		JavaPairRDD<Integer, Tuple2<Iterable<GeoPoint>, GeoKDTree>> groups = pointPairs
				.flatMapToPair(distributeF).partitionBy(partitioner)
				.cogroup(subTrees).mapValues(collectOnly);
		KNN1 knn1 = new KNN1(k);
		JavaPairRDD<Integer, GeoPoint> knn1R = groups.flatMapToPair(knn1);
		JavaPairRDD<Integer, GeoPoint> notReadyR = knn1R.filter(filterNotReady);
		JavaPairRDD<Integer, GeoPoint> readyR = knn1R.filter(filterReady);
		KNN2 knn2 = new KNN2(k);
		KNN2FlatMap notReadyMap = new KNN2FlatMap(this.top);
		JavaPairRDD<Integer, GeoPoint> knn2R = notReadyR
				.flatMapToPair(notReadyMap).partitionBy(partitioner)
				.cogroup(subTrees).mapValues(collectOnly).flatMapToPair(knn2);
		JavaPairRDD<Integer, GeoPoint> knn3TempR = notReadyR.union(knn2R);
		KNN3 knn3 = new KNN3(k);
		JavaPairRDD<Integer, GeoPoint> knn3R = knn3TempR.mapToPair(IDF)
				.groupByKey().mapToPair(knn3);

		JavaPairRDD<Integer, GeoPoint> knns = readyR.union(knn3R);
		return knns;
	}

	public JavaPairRDD<Integer, GeoPoint> epsilonNeighborhoodKNNQuery(
			JavaRDD<GeoPoint> points, int k, float epsilon) {

		JavaPairRDD<Integer, GeoPoint> pointsStream = changeToPair(points);
		DistributePointsToSingle distributeF = new DistributePointsToSingle(top);
		JavaPairRDD<Integer, Tuple2<Iterable<GeoPoint>, GeoKDTree>> groupsDStream = pointsStream
				.flatMapToPair(distributeF).partitionBy(partitioner)
				.cogroup(subTrees).mapValues(collectOnly);
		EpsilonNeighborhoodKNN eknn = new EpsilonNeighborhoodKNN(k, epsilon);
		JavaPairRDD<Integer, GeoPoint> eKNNs = groupsDStream.flatMapToPair(eknn);
		return eKNNs;
	}

	private JavaPairDStream<Integer, GeoPoint> changeToPairStream(
			JavaDStream<GeoPoint> points) {
		@SuppressWarnings("serial")
		JavaPairDStream<Integer, GeoPoint> pointsStream = points
				.mapToPair(new PairFunction<GeoPoint, Integer, GeoPoint>() {

					@Override
					public Tuple2<Integer, GeoPoint> call(GeoPoint arg0)
							throws Exception {
						return new Tuple2<Integer, GeoPoint>(0, arg0);
					}

				});
		return pointsStream;
	}

	@SuppressWarnings("serial")
	Function<JavaPairRDD<Integer, GeoPoint>, JavaPairRDD<Integer, Tuple2<Iterable<GeoPoint>, Iterable<GeoKDTree>>>> transformFunc = new Function<JavaPairRDD<Integer, GeoPoint>, JavaPairRDD<Integer, Tuple2<Iterable<GeoPoint>, Iterable<GeoKDTree>>>>() {

		@Override
		public JavaPairRDD<Integer, Tuple2<Iterable<GeoPoint>, Iterable<GeoKDTree>>> call(
				JavaPairRDD<Integer, GeoPoint> arg0) throws Exception {
			return arg0.partitionBy(partitioner).cogroup(subTrees);
		}

	};
	@SuppressWarnings("serial")
	Function<Tuple2<Iterable<GeoPoint>, Iterable<GeoKDTree>>, Tuple2<Iterable<GeoPoint>, GeoKDTree>> collectOnly = new Function<Tuple2<Iterable<GeoPoint>, Iterable<GeoKDTree>>, Tuple2<Iterable<GeoPoint>, GeoKDTree>>() {

		@Override
		public Tuple2<Iterable<GeoPoint>, GeoKDTree> call(
				Tuple2<Iterable<GeoPoint>, Iterable<GeoKDTree>> arg0)
				throws Exception {
			// TODO Auto-generated method stub
			return new Tuple2<Iterable<GeoPoint>, GeoKDTree>(arg0._1, arg0._2
					.iterator().next());
		}

	};

	@SuppressWarnings("serial")
	Function<Tuple2<Integer, GeoPoint>, Boolean> filterNotReady = new Function<Tuple2<Integer, GeoPoint>, Boolean>() {

		@Override
		public Boolean call(Tuple2<Integer, GeoPoint> arg0) throws Exception {
			return !arg0._2.isReady();
		}

	};
	@SuppressWarnings("serial")
	PairFunction<Tuple2<Integer, GeoPoint>, Long, GeoPoint> IDF = new PairFunction<Tuple2<Integer, GeoPoint>, Long, GeoPoint>() {
		@Override
		public Tuple2<Long, GeoPoint> call(Tuple2<Integer, GeoPoint> arg0)
				throws Exception {
			GeoPoint p = arg0._2;
			return new Tuple2<Long, GeoPoint>(p.getID(), p);
		}
	};

	@SuppressWarnings("serial")
	Function<Tuple2<Integer, GeoPoint>, Boolean> filterReady = new Function<Tuple2<Integer, GeoPoint>, Boolean>() {

		@Override
		public Boolean call(Tuple2<Integer, GeoPoint> arg0) throws Exception {

			return arg0._2.isReady();
		}

	};

	public GeoKDTreeTop getTop() {
		return top;
	}

	public JavaPairRDD<Integer, GeoKDTree> getSubTrees() {
		return subTrees;
	}

}
