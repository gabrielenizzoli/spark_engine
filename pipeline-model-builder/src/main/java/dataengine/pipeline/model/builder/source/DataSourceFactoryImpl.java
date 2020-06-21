package dataengine.pipeline.model.builder.source;

import dataengine.pipeline.core.DataFactoryException;
import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.factory.DataSourceFactory;
import dataengine.pipeline.model.pipeline.step.*;
import lombok.AllArgsConstructor;

import javax.annotation.Nonnull;

@AllArgsConstructor
public class DataSourceFactoryImpl implements DataSourceFactory {

    @Nonnull
    StepFactory stepsFactory;

    public static DataSourceFactoryImpl withStepFactory(StepFactory stepFactory) {
        return new DataSourceFactoryImpl(stepFactory);
    }

    @Override
    public DataSource apply(String name) {
        Step step = stepsFactory.apply(name);
        if (step instanceof Source) {
            Source source = (Source) step;
            return Components.buildDataSource(source);
        } else if (step instanceof SingleInputStep) {
            return createSourceForSingleInputStep((SingleInputStep) step);
        } else if (step instanceof MultiInputStep) {
            return createSourceForMultiInputStep((MultiInputStep) step);
        }
        throw new DataFactoryException("source " + step + " not managed");
    }

    private DataSource createSourceForSingleInputStep(SingleInputStep step) {
        return Components.createSourceForSingleInputStep(step, this);
    }

    private DataSource createSourceForMultiInputStep(MultiInputStep step) {
        return Components.createSourceForMultiInputStep(step, this);
    }

}
