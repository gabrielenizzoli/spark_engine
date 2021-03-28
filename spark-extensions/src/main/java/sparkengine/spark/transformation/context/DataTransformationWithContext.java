package sparkengine.spark.transformation.context;

import org.apache.spark.broadcast.Broadcast;

public interface DataTransformationWithContext {

    void setTransformationContext(Broadcast<DataTransformationContext> transformationContextBroadcast);

}
