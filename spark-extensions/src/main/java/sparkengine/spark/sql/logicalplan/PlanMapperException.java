package sparkengine.spark.sql.logicalplan;

public class PlanMapperException extends Exception {

    public PlanMapperException(String str) {
        super(str);
    }

    public PlanMapperException(String str, Throwable t) {
        super(str, t);
    }

}
