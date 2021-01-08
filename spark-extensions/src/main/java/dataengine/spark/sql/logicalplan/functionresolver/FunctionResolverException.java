package dataengine.spark.sql.logicalplan.functionresolver;

import dataengine.spark.sql.logicalplan.PlanMapperException;

public class FunctionResolverException extends PlanMapperException {

    public FunctionResolverException(String str) {
        super(str);
    }

    public FunctionResolverException(String str, Throwable t) {
        super(str, t);
    }

}
