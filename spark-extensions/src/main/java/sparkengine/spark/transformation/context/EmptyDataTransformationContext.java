package sparkengine.spark.transformation.context;

public class EmptyDataTransformationContext implements DataTransformationContext {

    @Override
    public void acc(String name, long value) {

    }

    @Override
    public String toString() {
        return "EMPTY";
    }

}
