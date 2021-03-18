package sparkengine.spark.sql.udf;

import sparkengine.spark.sql.logicalplan.PlanMapperException;

public class UdfCompilationException extends PlanMapperException {

    public UdfCompilationException(String str) {
        super(str);
    }

    public UdfCompilationException(String str, Throwable t) {
        super(str, t);
    }

}
