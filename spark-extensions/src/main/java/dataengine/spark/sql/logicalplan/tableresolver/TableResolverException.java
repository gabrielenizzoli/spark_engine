package dataengine.spark.sql.logicalplan.tableresolver;

import dataengine.spark.sql.logicalplan.PlanMapperException;

public class TableResolverException extends PlanMapperException {

    public TableResolverException(String str) {
        super(str);
    }

    public TableResolverException(String str, Throwable t) {
        super(str, t);
    }

}
