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
        sparkSession = SparkSession.builder().master("local").getOrCreate();
    }

    private static void windowsNoisyLogsWorkaround() {
        // workaround to remove noisy test info for windows
        try {
            File workaround = new File(".");
            System.getProperties().put("hadoop.home.dir", workaround.getAbsolutePath());
            new File("./bin").mkdirs();
            new File("./bin/winutils.exe").createNewFile();
        } catch (Exception ignore) {
            // just ignore any issue, downside is more noise
        }
    }

    @AfterAll
    static void close() {
        sparkSession.close();
    }

}
