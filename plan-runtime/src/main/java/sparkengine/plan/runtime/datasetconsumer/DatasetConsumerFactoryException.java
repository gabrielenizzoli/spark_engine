package sparkengine.plan.runtime.datasetconsumer;

public class DatasetConsumerFactoryException extends Exception {

    public DatasetConsumerFactoryException(String str) {
        super(str);
    }

    public DatasetConsumerFactoryException(String str, Throwable t) {
        super(str, t);
    }

    public static class ConsumerNotFound extends DatasetConsumerFactoryException {

        public ConsumerNotFound(String str) {
            super(str);
        }

    }

    public static class ConsumerInstantiationException extends DatasetConsumerFactoryException {

        public ConsumerInstantiationException(String str) {
            super(str);
        }

    }

    public static class UnmanagedParameter extends DatasetConsumerFactoryException {

        public UnmanagedParameter(String str) {
            super(str);
        }

    }

}
