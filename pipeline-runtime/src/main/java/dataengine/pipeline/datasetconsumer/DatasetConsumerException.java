package dataengine.pipeline.datasetconsumer;

public class DatasetConsumerException extends Exception {

    public DatasetConsumerException(String str) {
        super(str);
    }

    public DatasetConsumerException(String str, Throwable t) {
        super(str, t);
    }

}
