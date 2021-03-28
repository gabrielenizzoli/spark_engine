package sparkengine.spark.sql.udf.context;

import sparkengine.spark.Context;

import java.io.Serializable;

public interface UdfContext extends Context, Serializable {

    UdfContext EMPTY_UDF_CONTEXT = new EmptyUdfContext();

}
