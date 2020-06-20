package datangine.pipeline_builder;

public class PipelineBuilderException extends RuntimeException {

    public PipelineBuilderException(String str) {
        super(str);
    }

    public PipelineBuilderException(String str, Throwable t) {
        super(str, t);
    }

}
