package sparkengine.spark.transformation.context;

import sparkengine.spark.sql.udf.context.UdfContext;

public class EmptyTransformationContext implements TransformationContext {

    @Override
    public void acc(String name, long value) {

    }

    @Override
    public String toString() {
        return "EMPTY";
    }

}
