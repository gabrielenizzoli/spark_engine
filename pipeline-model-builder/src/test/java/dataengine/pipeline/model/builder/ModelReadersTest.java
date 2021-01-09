package dataengine.pipeline.model.builder;

import dataengine.pipeline.model.component.Component;
import dataengine.pipeline.model.component.catalog.ComponentCatalog;
import dataengine.pipeline.model.component.catalog.ComponentCatalogException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;

class ModelReadersTest {

    @Test
    void testYaml() throws IOException, ComponentCatalogException {

        File yamlSource = new File("src/test/resources/components.yaml");
        ComponentCatalog catalog = ModelReaders.readComponentMapFromYaml(() -> {
            try {
                return new FileInputStream(yamlSource);
            } catch (FileNotFoundException e) {
                throw new IllegalStateException();
            }
        });

        Optional<Component> sourceComponent = catalog.lookup("tx");

        Assertions.assertNotNull(sourceComponent);
        Assertions.assertTrue(sourceComponent.isPresent());

    }

}