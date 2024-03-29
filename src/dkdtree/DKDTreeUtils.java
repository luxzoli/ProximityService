package dkdtree;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import kdtree.Point;
import scala.Tuple2;

public class DKDTreeUtils {

	public static JavaRDD<Point> pointFromString(JavaRDD<String> pointStrings) {
		@SuppressWarnings("serial")
		JavaRDD<Point> points = pointStrings.map(new Function<String, Point>() {

			@Override
			public Point call(String arg0) throws Exception {
				// System.out.println(arg0);
				String pointString = arg0.substring(arg0.indexOf('\t') + 1);
				return new Point(pointString);
			}

		});
		return points;
	}

	public static JavaDStream<Point> undoPair(JavaPairDStream<Integer, Point> tKNNDStream) {
		@SuppressWarnings("serial")
		Function<Tuple2<Integer, Point>, Point> f = new Function<Tuple2<Integer, Point>, Point>() {

			@Override
			public Point call(Tuple2<Integer, Point> arg0) throws Exception {
				return arg0._2;
			}

		};
		JavaDStream<Point> knnDStream = tKNNDStream.map(f);
		return knnDStream;
	}

	public static JavaDStream<Point> pointFromStringStream(JavaDStream<String> inputStream) {
		@SuppressWarnings("serial")
		JavaDStream<Point> pointsStream = inputStream.map(new Function<String, Point>() {

			@Override
			public Point call(String arg0) throws Exception {
				// System.out.println(arg0);
				String pointString = arg0.substring(arg0.indexOf('\t') + 1);
				return new Point(pointString);
			}

		});

		return pointsStream;
	}
	
	
	public static void saveResLocal(String outPath, List<Point> points) {
		File f = new File(outPath);
		if (!f.exists()) {
			try {
				f.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} 
		try (PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(f,true)))) {
			for (Point p : points) {
				p.setReady(false);
				pw.println(p);
				p.setReady(true);
			}
			pw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("deprecation")
	public static void saveKNNResLocal(final String path, JavaDStream<Point> knnDStream) {
		@SuppressWarnings("serial")
		Function<JavaRDD<Point>, Void> saveResFunc = new Function<JavaRDD<Point>, Void>(){

			@Override
			public Void call(JavaRDD<Point> v1) throws Exception {
				DKDTreeUtils.saveResLocal(path, v1.collect());
				return null;
			}
			
		};
		knnDStream.foreachRDD(saveResFunc);
	}
	
}
