package main;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import geo.kdtree.GeoPoint;
import proximity.ProximityService;
import proximity.ProximityUtils;

public class ProximityDriver {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local[4]")
				.setAppName("SparkStreamingKDTree")
				.set("spark.streaming.backpressure.enabled", "true");
		String inPath = args[0];
		float epsilon = Float.parseFloat(args[1]);
		int k = Integer.parseInt(args[2]);
		int sampleSize = Integer.parseInt(args[3]);
		int numPartitions = Integer.parseInt(args[4]);
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc,
				Durations.milliseconds(6000));
		JavaDStream<String> inputStream = ssc.socketTextStream("localhost",
				13000);
		JavaRDD<String> pointStrings = sc.textFile(inPath);

		JavaRDD<GeoPoint> pointsNF = ProximityUtils.pointFromString(pointStrings);
		JavaRDD<GeoPoint> points = ProximityUtils.filterGeoPoints(pointsNF);

		JavaDStream<GeoPoint> pointsStream = ProximityUtils.pointFromStringStream(inputStream).persist(StorageLevel.MEMORY_ONLY());

		JavaDStream<GeoPoint> knnDStream = ProximityService.queryNearby(pointsStream, points, k, epsilon, numPartitions, sampleSize);
		//ProximityUtils.saveKNNRes(knnDStream);
		knnDStream.count().print();
		ssc.start();
		ssc.awaitTerminationOrTimeout(120000);
		ssc.close();
		ssc.stop();
		sc.stop();

	}

	
}
