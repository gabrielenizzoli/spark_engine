package sparkengine.spark.test;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SparkSessionManagerTest extends SparkSessionManager {

    @Test
    public void testSession() {
        Assertions.assertEquals("local", sparkSession.sparkContext().master());
    }

}
