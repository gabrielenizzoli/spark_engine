package datangine.pipeline_builder.source;

import dataengine.model.pipeline.step.MultiInputStep;
import dataengine.model.pipeline.step.SingleInputStep;
import dataengine.model.pipeline.step.Source;
import dataengine.model.pipeline.step.Step;
import dataengine.pipeline.DataSource;
import datangine.pipeline_builder.PipelineBuilderException;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

@Value
@Builder
public class DataSourceFactoryImpl implements DataSourceFactory {

    @Nonnull
    private Map<String, Step> steps;

    @Override
    public DataSource apply(String name) {
        Step step = steps.get(name);
        if (step instanceof Source) {
            Source source = (Source) step;
            return Components.buildDataSource(source);
        } else if (step instanceof SingleInputStep) {
            return createSourceForSingleInputStep((SingleInputStep) step);
        } else if (step instanceof MultiInputStep) {
            return createSourceForMultiInputStep((MultiInputStep) step);
        }
        throw new PipelineBuilderException("source " + step + " not managed");
    }

    private DataSource createSourceForSingleInputStep(SingleInputStep step) {
        return Components.createSourceForSingleInputStep(step, this);
    }

    private DataSource createSourceForMultiInputStep(MultiInputStep step) {
        return Components.createSourceForMultiInputStep(step, this);
    }

}
