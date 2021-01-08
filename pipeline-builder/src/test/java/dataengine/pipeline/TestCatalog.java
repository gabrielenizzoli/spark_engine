package dataengine.pipeline;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dataengine.pipeline.model.sink.Sink;
import dataengine.pipeline.model.sink.catalog.SinkCatalog;
import dataengine.pipeline.model.sink.catalog.SinkCatalogException;
import dataengine.pipeline.model.component.Component;
import dataengine.pipeline.model.component.catalog.ComponentCatalog;
import dataengine.pipeline.model.component.catalog.ComponentCatalogException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.util.Map;
import java.util.Optional;

public class TestCatalog {

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
                    File yamlSource = new File("src/test/resources/" + resourceName + ".yaml");
                    cachedSteps = mapper.readValue(yamlSource, new TypeReference<Map<String, Component>>() {
                    });
                } catch (Exception e) {
                    throw new ComponentCatalogException("can't build", e);
                }
            }

        };
    }

    @Nonnull
    public static SinkCatalog getSinkCatalog(@Nullable String resourceName) {
        return new SinkCatalog() {

            private Map<String, Sink> cachedSteps;

            @Override
            @Nonnull
            public Optional<Sink> lookup(String name) throws SinkCatalogException {
                if (cachedSteps == null)
                    read();
                return Optional.ofNullable(cachedSteps.get(name));
            }

            public void read() throws SinkCatalogException {
                try {
                    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
                    File yamlSource = new File("src/test/resources/" + resourceName + ".yaml");
                    cachedSteps = mapper.readValue(yamlSource, new TypeReference<Map<String, Sink>>() {
                    });
                } catch (Exception e) {
                    throw new SinkCatalogException("can't build", e);
                }
            }

        };
    }

}
