package sparkengine.spark.sql.logicalplan.functionresolver;

import sparkengine.spark.sql.logicalplan.PlanMapperException;

public class FunctionResolverException extends PlanMapperException {

    public FunctionResolverException(String str) {
        super(str);
    }

    public FunctionResolverException(String str, Throwable t) {
        super(str, t);
    }

}
