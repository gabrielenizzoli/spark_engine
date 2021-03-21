package sparkengine.spark.sql.udf;

import org.apache.spark.broadcast.Broadcast;

public interface UdfWithContext {

    void setUdfContext(Broadcast<UdfContext> udfContext);

}
