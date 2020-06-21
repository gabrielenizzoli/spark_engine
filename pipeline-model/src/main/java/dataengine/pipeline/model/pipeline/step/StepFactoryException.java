package dataengine.pipeline.model.pipeline.step;

public class StepFactoryException extends RuntimeException {

    public StepFactoryException(String str) {
        super(str);
    }

    public StepFactoryException(String str, Throwable t) {
        super(str, t);
    }

}
