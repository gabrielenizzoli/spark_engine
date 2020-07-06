package dataengine.spark.test;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class SparkSessionBase {

    protected static SparkSession sparkSession;

    @BeforeAll
    static void init() {
        sparkSession = SparkSession.builder().master("local").getOrCreate();
    }

    @AfterAll
    static void close() {
        sparkSession.close();
    }

}
