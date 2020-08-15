package dataengine.spark.test;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.File;
import java.io.IOException;

public class SparkSessionBase {

    protected static SparkSession sparkSession;

    @BeforeAll
    static void init() throws IOException {
        windowsNoisyLogsWorkaround();
        //if (SparkSession.getActiveSession().nonEmpty())
        //    throw new IOException("session already exists");
        sparkSession = SparkSession.builder().master("local").config("spark.sql.streaming.forceDeleteTempCheckpointLocation", true).getOrCreate();
    }

    private static void windowsNoisyLogsWorkaround() {
        try {
            File workaround = new File(System.getenv("HADOOP_HOME")).getCanonicalFile();
            System.getProperties().put("hadoop.home.dir", workaround.getAbsolutePath());
        } catch (Exception ignore) {
            // just ignore any issue, downside is more noise
        }
    }

    @AfterAll
    static void close() {
        sparkSession.close();
    }

}
