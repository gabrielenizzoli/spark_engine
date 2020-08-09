package dataengine.spark.sql.udf;

import dataengine.spark.sql.PlanMapperException;

public class FunctionResolverException extends PlanMapperException {

    public FunctionResolverException(String str) {
        super(str);
    }

    public FunctionResolverException(String str, Throwable t) {
        super(str, t);
    }

}
