package main;

import kdtree.Point;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import dkdtree.DKDTree;
import dkdtree.DKDTreeUtils;

public class StreamingDriver {

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setMaster("local[4]")
				.setAppName("SparkStreamingKDTree")
				.set("spark.streaming.backpressure.enabled", "true");

		String inPath = args[0];
		String host = args[1];
		int k = Integer.parseInt(args[2]);
		int sampleSize = Integer.parseInt(args[3]);
		int numPartitions = Integer.parseInt(args[4]);
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc,
				Durations.milliseconds(6000));
		JavaDStream<String> inputStream = ssc.socketTextStream(host, 13000);

		JavaRDD<String> pointStrings = sc.textFile(inPath);
		JavaRDD<Point> points = DKDTreeUtils.pointFromString(pointStrings);

		// This version should be used when possible, it is much faster
		JavaDStream<Point> pointsStream = DKDTreeUtils.pointFromStringStream(
				inputStream).persist(StorageLevel.MEMORY_ONLY());
		JavaDStream<Point> ekNNDStream = DKDTree
				.streamEpsilonNeighborhoodKNNQuery(pointsStream, points, k,
						0.015f, numPartitions, sampleSize);

		//DKDTreeUtils.saveKNNResLocal("nres.txt", ekNNDStream);
		ekNNDStream.count().print();

		JavaDStream<Point> kNNDStream = DKDTree.streamKNNQuery(pointsStream,
				points, k, numPartitions, sampleSize);

		kNNDStream.count().print();
		ssc.start();
		ssc.awaitTerminationOrTimeout(60000);
		ssc.stop();
		sc.stop();
		ssc.close();
		sc.close();

	}

}
