package dataengine.pipeline.model.builder.source;

import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.model.pipeline.step.StepFactory;
import lombok.Getter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.storage.StorageLevel;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class DataSourceFactoryDebug extends DataSourceFactoryImpl {

    @Getter
    private final LinkedHashMap<String, DataSource<?>> accumulatedDataSources = new LinkedHashMap<>();
    @Getter
    private final Map<String, Dataset<?>> accumulatedDataSets = new HashMap<>();

    private DataSourceFactoryDebug(StepFactory stepsFactory) {
        super(stepsFactory);
    }

    public static DataSourceFactoryDebug withStepFactory(StepFactory stepFactory) {
        return new DataSourceFactoryDebug(stepFactory);
    }

    @Override
    public DataSource<?> apply(String name) {
        DataSource<?> ds = super.apply(name).cache(StorageLevel.MEMORY_ONLY());
        accumulatedDataSources.put(name, ds);
        return ds;
    }

    public Dataset<?> getOrRun(String name) {

        // init datasources (lookup name and have the apply method accumulate information, up to 'name')
        apply(name);
        // validate
        if (!accumulatedDataSources.containsKey(name))
            throw new NullPointerException("datasource with name is not defined: " + name);

        if (!accumulatedDataSets.containsKey(name)) {
            for (String currentName : accumulatedDataSources.keySet()) {
                if (accumulatedDataSets.containsKey(currentName))
                    continue;
                Dataset<?> ds = accumulatedDataSources.get(currentName).get();
                ds.count();
                accumulatedDataSets.put(currentName, ds);
                if (currentName.equals(name))
                    break;
            }
        }
        return accumulatedDataSets.get(name);
    }

}
