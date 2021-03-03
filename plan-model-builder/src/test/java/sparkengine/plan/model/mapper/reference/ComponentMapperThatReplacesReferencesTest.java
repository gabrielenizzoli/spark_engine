package sparkengine.plan.model.mapper.reference;

import org.junit.jupiter.api.Test;
import sparkengine.plan.model.common.Location;
import sparkengine.plan.model.common.Reference;
import sparkengine.plan.model.builder.input.FileResourceLocator;
import sparkengine.plan.model.component.impl.ReferenceComponent;
import sparkengine.plan.model.component.impl.SqlComponent;
import sparkengine.plan.model.builder.ResourceLocationBuilder;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ComponentMapperThatReplacesReferencesTest {

    @Test
    void testRelativeReference() throws Exception {

        // given
        var resourceLocationBuilder = new ResourceLocationBuilder("src/test/resources/referenceComponentMapperTest/plan.yaml", "_", "yaml");
        var referenceComponentMapper = ComponentMapperThatReplacesReferences.of(resourceLocationBuilder, new FileResourceLocator());
        var referenceComponent = ReferenceComponent.builder().withMode(Reference.ReferenceMode.RELATIVE).build();
        var location = Location.of("location");

        // when
        var referencedComponent = referenceComponentMapper.mapReferenceComponent(location, referenceComponent);

        // then
        assertEquals(SqlComponent.TYPE_NAME, referencedComponent.componentTypeName());
    }

    @Test
    void testRelativeReferenceWithMultipleLocations() throws Exception {

        // given
        var resourceLocationBuilder = new ResourceLocationBuilder("src/test/resources/referenceComponentMapperTest/plan.yaml", "_", "yaml");
        var referenceComponentMapper = ComponentMapperThatReplacesReferences.of(resourceLocationBuilder, new FileResourceLocator());
        var referenceComponent = ReferenceComponent.builder().withMode(Reference.ReferenceMode.RELATIVE).build();
        var location = Location.of("location1", "location2");

        // when
        var referencedComponent = referenceComponentMapper.mapReferenceComponent(location, referenceComponent);

        // then
        assertEquals(SqlComponent.TYPE_NAME, referencedComponent.componentTypeName());
    }

    @Test
    void testRelativeReferenceWithMissingLocations() throws Exception {

        // given
        var resourceLocationBuilder = new ResourceLocationBuilder("src/test/resources/referenceComponentMapperTest/plan.yaml", "_", "yaml");
        var referenceComponentMapper = ComponentMapperThatReplacesReferences.of(resourceLocationBuilder, new FileResourceLocator());
        var referenceComponent = ReferenceComponent.builder().withMode(Reference.ReferenceMode.RELATIVE).build();
        var location = Location.of("location2", "location1");

        // then
        assertThrows(IOException.class, () -> referenceComponentMapper.mapReferenceComponent(location, referenceComponent));
    }

    @Test
    void testAbsoluteReference() throws Exception {

        // given
        var resourceLocationBuilder = new ResourceLocationBuilder("src/test/resources/referenceComponentMapperTest/plan_location.yaml", "_", "yaml");
        var referenceComponentMapper = ComponentMapperThatReplacesReferences.of(resourceLocationBuilder, new FileResourceLocator());
        var referenceComponent = ReferenceComponent.builder()
                .withMode(Reference.ReferenceMode.ABSOLUTE)
                .withRef("src/test/resources/referenceComponentMapperTest/plan_location.yaml")
                .build();
        var location = Location.of("another-location");

        // when
        var referencedComponent = referenceComponentMapper.mapReferenceComponent(location, referenceComponent);

        // then
        assertEquals(SqlComponent.TYPE_NAME, referencedComponent.componentTypeName());
    }

}