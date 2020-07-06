package dataengine.pipeline.model.supplier;

import dataengine.pipeline.model.description.source.Component;
import dataengine.pipeline.model.description.source.ComponentCatalogException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

class ComponentCatalogYamlMapTest {

    @Test
    void testYaml() throws ComponentCatalogException {

        File yamlSource = new File("src/test/resources/simplePipeline.yaml");
        ComponentCatalogFromYamlMap catalog = ComponentCatalogFromYamlMap.builder().inputStreamFactory(() -> {
            try {
                return new FileInputStream(yamlSource);
            } catch (FileNotFoundException e) {
                throw new IllegalStateException();
            }
        }).build();

        Component sourceComponent = catalog.lookup("tx");

        Assertions.assertNotNull(sourceComponent);
    }

}