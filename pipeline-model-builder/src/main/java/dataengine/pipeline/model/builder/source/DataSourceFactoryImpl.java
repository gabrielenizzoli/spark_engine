package dataengine.pipeline.model.builder.source;

import dataengine.pipeline.model.builder.ModelBuilderException;
import dataengine.pipeline.model.pipeline.step.MultiInputStep;
import dataengine.pipeline.model.pipeline.step.SingleInputStep;
import dataengine.pipeline.model.pipeline.step.Source;
import dataengine.pipeline.model.pipeline.step.Step;
import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.factory.DataSourceFactory;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
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
        throw new ModelBuilderException("source " + step + " not managed");
    }

    private DataSource createSourceForSingleInputStep(SingleInputStep step) {
        return Components.createSourceForSingleInputStep(step, this);
    }

    private DataSource createSourceForMultiInputStep(MultiInputStep step) {
        return Components.createSourceForMultiInputStep(step, this);
    }

}
