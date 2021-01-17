package dataengine.pipeline.runtime.plan;

public class PlanFactoryException extends Exception {

    public PlanFactoryException(String str) {
        super(str);
    }

    public PlanFactoryException(String str, Throwable t) {
        super(str, t);
    }

    public static class PipelineNotFound extends PlanFactoryException {

        public PipelineNotFound(String str) {
            super(str);
        }

    }

}
