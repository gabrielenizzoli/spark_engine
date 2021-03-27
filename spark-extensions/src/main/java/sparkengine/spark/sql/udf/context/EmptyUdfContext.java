package sparkengine.spark.sql.udf.context;

public class EmptyUdfContext implements UdfContext {

    @Override
    public void acc(String name, long value) {

    }

    @Override
    public String toString() {
        return "EMPTY";
    }
}
