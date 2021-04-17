package sparkengine.spark.sql.udf.context;

import org.apache.spark.broadcast.Broadcast;

public interface UdfWithContext {

    void setUdfContextBroadcast(Broadcast<UdfContext> udfContextBroadcast);

}
