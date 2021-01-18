package sparkengine.spark.transformation;

public class TransformationException extends RuntimeException {

    public TransformationException(String str) {
        super(str);
    }

    public TransformationException(String str, Throwable t) {
        super(str, t);
    }


}
