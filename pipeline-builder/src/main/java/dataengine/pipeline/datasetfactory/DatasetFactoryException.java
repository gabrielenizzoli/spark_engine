package dataengine.pipeline.datasetfactory;

public class DatasetFactoryException extends Exception {

    public DatasetFactoryException(String str) {
        super(str);
    }

    public DatasetFactoryException(String str, Throwable t) {
        super(str, t);
    }

    public static class ComponentNotFound extends DatasetFactoryException {

        public ComponentNotFound(String str) {
            super(str);
        }

    }

    public static class ComponentNotManaged extends DatasetFactoryException {

        public ComponentNotManaged(String str) {
            super(str);
        }

    }

    public static class CircularReference extends DatasetFactoryException {

        public CircularReference(String str) {
            super(str);
        }

    }

}
