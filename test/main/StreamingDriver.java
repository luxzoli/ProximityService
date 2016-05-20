package main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import dkdtree.DKDtreeUtils;

import dkdtree.DKDTree;
import kdtree.Point;
import scala.Tuple2;

public class StreamingDriver {

	public static void main(String[] args) {
		// Logger.getLogger("org").setLevel(Level.OFF);
		// Logger.getLogger("akka").setLevel(Level.OFF);
		SparkConf sparkConf = new SparkConf().setMaster("local[4]")
				.setAppName("SparkStreamingKDTree")
				// .set("spark.eventLog.enabled", "false")
				// .set("spark.streaming.backpressure.enabled", "true")
		;
		
		String inPath = args[0];
		String host = args[1];
		int k = Integer.parseInt(args[2]);
		int sampleSize = Integer.parseInt(args[3]);
		int numPartitions = Integer.parseInt(args[4]);
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.milliseconds(6000));
		JavaDStream<String> inputStream = ssc.socketTextStream(host, 13000);

		JavaRDD<String> pointStrings = sc.textFile(inPath);
		JavaRDD<Point> points = DKDtreeUtils.pointFromString(pointStrings);

		//This version should be used, it is much faster
		JavaDStream<Point> pointsStream = DKDtreeUtils.pointFromStringStream(inputStream).persist(StorageLevel.MEMORY_ONLY());
		JavaDStream<Point> ekNNDStream = DKDTree.streamEpsilonNeighborhoodKNNQuery(pointsStream, points, k, 0.015f,
				numPartitions, sampleSize);
		
		ekNNDStream.count().print();

		JavaDStream<Point> kNNDStream = DKDTree.streamKNNQuery(pointsStream, points, k, numPartitions, sampleSize);

		
		kNNDStream.count().print();
		ssc.start();
		ssc.awaitTerminationOrTimeout(180000);
		ssc.stop();
		sc.stop();
		ssc.close();
		sc.close();

	}

	
}
