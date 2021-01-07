package dataengine.pipeline.datasetconsumer;

public class DatasetConsumerException extends Exception {

    public DatasetConsumerException(String str) {
        super(str);
    }

    public DatasetConsumerException(String str, Throwable t) {
        super(str, t);
    }

    public static class SinkNotFound extends DatasetConsumerException {

        public SinkNotFound(String str) {
            super(str);
        }

    }

    public static class SinkNotManaged extends DatasetConsumerException {

        public SinkNotManaged(String str) {
            super(str);
        }

    }

}
