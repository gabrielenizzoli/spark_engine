package dataengine.pipeline.core;

public class DataFactoryException extends RuntimeException {

    public DataFactoryException(String str) {
        super(str);
    }

    public DataFactoryException(String str, Throwable t) {
        super(str, t);
    }

}
