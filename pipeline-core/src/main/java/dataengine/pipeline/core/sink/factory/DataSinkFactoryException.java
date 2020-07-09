package dataengine.pipeline.core.sink.factory;

public class DataSinkFactoryException extends Exception {

    public DataSinkFactoryException(String str) {
        super(str);
    }

    public DataSinkFactoryException(String str, Throwable t) {
        super(str, t);
    }

}
