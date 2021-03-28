package sparkengine.spark.transformation.context;

import sparkengine.spark.Context;

import java.io.Serializable;

public interface DataTransformationContext extends Context, Serializable {

    DataTransformationContext EMPTY_UDF_CONTEXT = new EmptyDataTransformationContext();

}
