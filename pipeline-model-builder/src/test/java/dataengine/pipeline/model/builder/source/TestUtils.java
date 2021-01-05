package dataengine.pipeline.model.builder.source;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dataengine.pipeline.core.Pipeline;
import dataengine.pipeline.core.consumer.catalog.DatasetConsumerCatalog;
import dataengine.pipeline.core.consumer.catalog.DatasetConsumerCatalogImpl;
import dataengine.pipeline.core.supplier.catalog.DatasetSupplierCatalog;
import dataengine.pipeline.core.supplier.catalog.DatasetSupplierCatalogImpl;
import dataengine.pipeline.model.description.sink.Sink;
import dataengine.pipeline.model.description.sink.SinkCatalog;
import dataengine.pipeline.model.description.sink.SinkCatalogException;
import dataengine.pipeline.model.description.source.Component;
import dataengine.pipeline.model.description.source.ComponentCatalog;
import dataengine.pipeline.model.description.source.ComponentCatalogException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.util.Map;
import java.util.Optional;

public class TestUtils {

    public static SinkCatalog getSinkCatalog() {
        return new SinkCatalog() {

            private Map<String, Sink> cachedSinks;

            @Nonnull
            @Override
            public Optional<Sink> lookup(String sinkName) throws SinkCatalogException {
                if (cachedSinks == null)
                    read();
                return Optional.ofNullable(cachedSinks.get(sinkName));
            }

            private void read() throws SinkCatalogException {
                try {
                    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
                    File yamlSource = new File("src/test/resources/testSinks.yaml");
                    cachedSinks = mapper.readValue(yamlSource, new TypeReference<Map<String, Sink>>() {
                    });
                } catch (Exception e) {
                    throw new SinkCatalogException("can't build", e);
                }
            }

        };
    }

    @Nonnull
    public static ComponentCatalog getComponentCatalog(@Nullable String resourceName) {
        return new ComponentCatalog() {

            private Map<String, Component> cachedSteps;

            @Override
            @Nonnull
            public Optional<Component> lookup(String name) throws ComponentCatalogException {
                if (cachedSteps == null)
                    read();
                return Optional.ofNullable(cachedSteps.get(name));
            }

            public void read() throws ComponentCatalogException {
                try {
                    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
                    File yamlSource = new File("src/test/resources/" + Optional.ofNullable(resourceName).orElse("testComponentsCatalog") + ".yaml");
                    cachedSteps = mapper.readValue(yamlSource, new TypeReference<Map<String, Component>>() {
                    });
                } catch (Exception e) {
                    throw new ComponentCatalogException("can't build", e);
                }
            }

        };
    }

    public static Pipeline getPipeline(@Nullable String resourceName) {
        DatasetSupplierCatalog DatasetSupplierCatalog = DatasetSupplierCatalogImpl.ofCatalog(TestUtils.getComponentCatalog(resourceName));
        DatasetConsumerCatalog datasetConsumerCatalog = DatasetConsumerCatalogImpl.ofCatalog(TestUtils.getSinkCatalog());

        return Pipeline.builder()
                .datasetSupplierCatalog(DatasetSupplierCatalog)
                .datasetConsumerCatalog(datasetConsumerCatalog)
                .build();
    }

}
