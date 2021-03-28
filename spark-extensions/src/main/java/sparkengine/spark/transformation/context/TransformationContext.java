package sparkengine.spark.transformation.context;

import sparkengine.spark.Context;
import sparkengine.spark.sql.udf.context.EmptyUdfContext;

import java.io.Serializable;

public interface TransformationContext extends Context, Serializable {

    TransformationContext EMPTY_UDF_CONTEXT = new EmptyTransformationContext();

}
