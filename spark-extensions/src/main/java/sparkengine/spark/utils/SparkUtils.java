package sparkengine.spark.utils;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

public class SparkUtils {

    private SparkUtils() {
    }

    public static <T> Broadcast<T> broadcast(SparkSession sparkSession, T item) {
        return new JavaSparkContext(sparkSession.sparkContext()).broadcast(item);
    }

    public static LongAccumulator longAccumulator(SparkSession sparkSession, String name) {
        return sparkSession.sparkContext().longAccumulator(name);
    }

    public static LongAccumulator longAnonymousAccumulator(SparkSession sparkSession) {
        return sparkSession.sparkContext().longAccumulator();
    }

}
