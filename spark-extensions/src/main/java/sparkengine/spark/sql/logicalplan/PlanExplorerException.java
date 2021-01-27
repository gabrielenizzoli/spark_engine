package sparkengine.spark.sql.logicalplan;

public class PlanExplorerException extends Exception {

    public PlanExplorerException(String str) {
        super(str);
    }

    public PlanExplorerException(String str, Throwable t) {
        super(str, t);
    }

}
