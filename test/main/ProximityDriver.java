package main;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import geo.kdtree.GeoPoint;
import proximity.ProximityService;
import proximity.ProximityUtils;

public class ProximityDriver {
	public static void main(String[] args) {
		//Logger.getLogger("org").setLevel(Level.OFF);
		//Logger.getLogger("akka").setLevel(Level.OFF);
		//long bTime = System.currentTimeMillis();
		SparkConf sparkConf = new SparkConf()//.setMaster("local[*]")
				.setAppName("SparkStreamingKDTree")
				//.set("spark.eventLog.enabled", "false")
				.set("spark.streaming.backpressure.enabled", "true");
		String inPath = args[0];
		String outPath = args[1];
		float epsilon = Float.parseFloat(args[2]);
		int k = Integer.parseInt(args[3]);
		int sampleSize = Integer.parseInt(args[4]);
		int numPartitions = Integer.parseInt(args[5]);
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc,
				Durations.milliseconds(10000));
		JavaDStream<String> inputStream = ssc.socketTextStream("localhost",
				13000);
		JavaRDD<String> pointStrings = sc.textFile(inPath);

		JavaRDD<GeoPoint> pointsNF = ProximityUtils.pointFromString(pointStrings);
		JavaRDD<GeoPoint> points = ProximityUtils.filterGeoPoints(pointsNF);

		JavaDStream<GeoPoint> pointsStream = ProximityUtils.pointFromStringStream(inputStream);

		JavaDStream<GeoPoint> knnDStream = ProximityService.queryNearby(pointsStream, points, k, epsilon, numPartitions, sampleSize);
		knnDStream.count().print();
		ssc.start();
		ssc.awaitTerminationOrTimeout(180000);
		ssc.stop();
		ssc.close();
		sc.stop();

	}
}
