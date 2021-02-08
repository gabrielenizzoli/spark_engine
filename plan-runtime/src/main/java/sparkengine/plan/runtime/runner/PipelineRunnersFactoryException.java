package sparkengine.plan.runtime.runner;

public class PipelineRunnersFactoryException extends Exception {

    public PipelineRunnersFactoryException(String str) {
        super(str);
    }

    public PipelineRunnersFactoryException(String str, Throwable t) {
        super(str, t);
    }

    public static class PipelineNotFound extends PipelineRunnersFactoryException {

        public PipelineNotFound(String str) {
            super(str);
        }

    }

}
