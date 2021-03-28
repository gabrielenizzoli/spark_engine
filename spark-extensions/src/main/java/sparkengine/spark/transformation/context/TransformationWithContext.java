package sparkengine.spark.transformation.context;

import org.apache.spark.broadcast.Broadcast;
import sparkengine.spark.sql.udf.context.UdfContext;

public interface TransformationWithContext {

    void setTransformationContext(Broadcast<TransformationContext> udfContext);

}
