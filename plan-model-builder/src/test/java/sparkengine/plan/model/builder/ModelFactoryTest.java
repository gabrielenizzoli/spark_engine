package sparkengine.plan.model.builder;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.catalog.ComponentCatalog;
import sparkengine.plan.model.component.catalog.ComponentCatalogException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Optional;

class ModelFactoryTest {

    @Test
    void testYaml() throws IOException, ComponentCatalogException, ModelFormatException {


        // given
        File yamlSource = new File("src/test/resources/components.yaml");
        ComponentCatalog catalog = ModelFactory.readComponentCatalogFromYaml(() -> new FileInputStream(yamlSource));

        // when
        Optional<Component> sourceComponent = catalog.lookup("tx");

        // then
        Assertions.assertNotNull(sourceComponent);
        Assertions.assertTrue(sourceComponent.isPresent());
    }

}