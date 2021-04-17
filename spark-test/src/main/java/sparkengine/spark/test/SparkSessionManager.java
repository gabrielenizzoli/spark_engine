package sparkengine.spark.test;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class SparkSessionManager {

    public static Map<String, String> SPARK_TEST_PROPERTIES = Map.ofEntries(
            Map.entry("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true"),
            Map.entry("spark.driver.host", "127.0.0.1"),
            Map.entry("spark.driver.bindAddress", "127.0.0.1")
    );

    protected static SparkSession sparkSession;

    @BeforeAll
    static void init() throws IOException {
        windowsNoisyLogsWorkaround();
        var builder = SparkSession.builder().master("local");
        SPARK_TEST_PROPERTIES.forEach((k,v) -> builder.config(k, v));
        sparkSession = builder.getOrCreate();
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
        SparkSession.clearActiveSession();
    }

    @BeforeEach
    public void initEach() {
        sparkSession.catalog().clearCache();
        sparkSession.catalog().listTables().collectAsList().stream().
                filter(table -> table.tableType().equalsIgnoreCase("view"))
                .forEach(table -> sparkSession.catalog().dropTempView(table.tableType()));
    }

}
