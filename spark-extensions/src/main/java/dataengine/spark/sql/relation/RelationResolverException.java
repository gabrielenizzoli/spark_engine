package dataengine.spark.sql.relation;

import dataengine.spark.sql.PlanMapperException;

public class RelationResolverException extends PlanMapperException {

    public RelationResolverException(String str) {
        super(str);
    }

    public RelationResolverException(String str, Throwable t) {
        super(str, t);
    }

}
