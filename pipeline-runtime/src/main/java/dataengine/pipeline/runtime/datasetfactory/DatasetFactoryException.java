package dataengine.pipeline.runtime.datasetfactory;

public class DatasetFactoryException extends Exception {

    public DatasetFactoryException(String str) {
        super(str);
    }

    public DatasetFactoryException(String str, Throwable t) {
        super(str, t);
    }

    public static class DatasetNotFound extends DatasetFactoryException {

        public DatasetNotFound(String str) {
            super(str);
        }

    }

    public static class DatasetInstantiationIssue extends DatasetFactoryException {

        public DatasetInstantiationIssue(String str) {
            super(str);
        }

    }

    public static class DatasetCircularReference extends DatasetFactoryException {

        public DatasetCircularReference(String str) {
            super(str);
        }

    }

}
