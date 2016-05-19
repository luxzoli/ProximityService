package proximity;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import geo.kdtree.GeoPoint;
import scala.Tuple2;

public class ProximityUtils {
	@SuppressWarnings("serial")
	public static JavaRDD<GeoPoint> pointFromString(JavaRDD<String> pointStrings) {
		JavaRDD<GeoPoint> points = pointStrings.map(new Function<String, GeoPoint>() {

			@Override
			public GeoPoint call(String arg0) throws Exception {
				String pointString = arg0.substring(arg0.indexOf('\t') + 1);
				return new GeoPoint(pointString);
			}

		});
		return points;
	}

	public static JavaDStream<GeoPoint> undoPair(
			JavaPairDStream<Integer, GeoPoint> tKNNDStream) {
		@SuppressWarnings("serial")
		Function<Tuple2<Integer, GeoPoint>, GeoPoint> f = new Function<Tuple2<Integer, GeoPoint>, GeoPoint>() {

			@Override
			public GeoPoint call(Tuple2<Integer, GeoPoint> arg0) throws Exception {
				return arg0._2;
			}

		};
		JavaDStream<GeoPoint> knnDStream = tKNNDStream.map(f);
		return knnDStream;
	}

	public static JavaDStream<GeoPoint> pointFromStringStream(
			JavaDStream<String> inputStream) {
		@SuppressWarnings("serial")
		JavaDStream<GeoPoint> pointsStream = inputStream
				.map(new Function<String, GeoPoint>() {

					@Override
					public GeoPoint call(String arg0) throws Exception {
						String pointString = arg0.substring(arg0.indexOf('\t') + 1);
						return new GeoPoint(pointString);
					}

				});

		return pointsStream;
	}

	@SuppressWarnings("serial")
	public static Function<GeoPoint, Boolean> filterGeoCoordinates = new Function<GeoPoint, Boolean>() {

		@Override
		public Boolean call(GeoPoint arg0) throws Exception {

			return ((arg0.getLat() <= 90f) && (arg0.getLat() >= -90f))
					&& ((arg0.getLon() <= 180f) && (arg0.getLon() >= -180f));
		}

	};
	
	public static JavaDStream<GeoPoint> filterGeoPointsStream(
			JavaDStream<GeoPoint> inputStream) {
		return inputStream.filter(filterGeoCoordinates);
	}
	
	public static JavaRDD<GeoPoint> filterGeoPoints(
			JavaRDD<GeoPoint> inputStream) {
		return inputStream.filter(filterGeoCoordinates);
	}

	public static void saveResLocal(String outPath, List<String> lines) {
		File f = new File(outPath, "out.txt");
		if (!f.exists()) {
			try {
				f.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(f))) {
			for (String line : lines) {
				bw.write(line + "\n");
			}
			bw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
