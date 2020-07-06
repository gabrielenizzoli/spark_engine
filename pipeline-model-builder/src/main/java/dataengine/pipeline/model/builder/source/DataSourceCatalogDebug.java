package dataengine.pipeline.model.builder.source;

import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.factory.DataSourceCatalogException;
import dataengine.pipeline.core.source.factory.DataSourceFactory;
import dataengine.pipeline.model.description.source.ComponentCatalog;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.storage.StorageLevel;

import java.util.HashMap;
import java.util.Map;

public class DataSourceCatalogDebug /*extends DataSourceCatalogImpl*/ {

    /*
    @Getter
    private final Map<String, Dataset<?>> precomputedDatasets = new HashMap<>();

    private DataSourceCatalogDebug(ComponentCatalog stepsFactory) {
        super(stepsFactory);
    }

    public static DataSourceCatalogDebug withStepFactory(ComponentCatalog sourceComponentCatalog) {
        return new DataSourceCatalogDebug(sourceComponentCatalog);
    }

    @Override
    public DataSourceFactory<?> apply(String name) {
        if (!precomputedDatasets.containsKey(name)) {
            DataSourceFactory<?> factory = super.apply(name);
            DataSource<?> dataSource = factory.build().cache(StorageLevel.MEMORY_ONLY());
            Dataset<?> dataset = dataSource.get();
            dataSource.get().count();
            precomputedDatasets.put(name, dataset);
        }
        return () -> (() -> precomputedDatasets.get(name));
    }

    public Dataset<?> getOrRun(String name) {

        // init datasources (lookup name and have the apply method accumulate information, up to 'name')
        apply(name);
        // validate
        if (!precomputedDataSources.containsKey(name))
            throw new NullPointerException("no datasource with name: " + name);

        if (!precomputedDataSets.containsKey(name)) {
            for (String currentName : precomputedDataSources.keySet()) {
                if (precomputedDataSets.containsKey(currentName))
                    continue;
                Dataset<?> ds = precomputedDataSources.get(currentName).get();
                ds.count();
                precomputedDataSets.put(currentName, ds);
                if (currentName.equals(name))
                    break;
            }
        }
        return precomputedDataSets.get(name);
    }

     */

}
