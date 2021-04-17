package sparkengine.spark.test;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.io.IOException;

public class SparkSessionManager {

    protected static SparkSession sparkSession;

    @BeforeAll
    static void init() throws IOException {
        windowsNoisyLogsWorkaround();
        sparkSession = SparkSession.builder().master("local")
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", true)
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();
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
