package sparkengine.plan.model.mapper;

import org.junit.jupiter.api.Test;
import sparkengine.plan.model.builder.ModelFactory;
import sparkengine.plan.model.builder.ModelFormatException;
import sparkengine.plan.model.builder.ResourceLocationBuilder;
import sparkengine.plan.model.builder.input.FileResourceLocator;
import sparkengine.plan.model.component.impl.WrapperComponent;
import sparkengine.plan.model.mapper.reference.ReferenceComponentMapper;
import sparkengine.plan.model.mapper.reference.ReferencePlanMapper;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class PlanMapperForComponentsTest {

    @Test
    void testReferenceMapper() throws IOException, ModelFormatException, PlanMapperException {

        // given
        var planLocation = "src/test/resources/componentsPlanMapperTest/nestedPlan.yaml";
        var resourceLocator = new FileResourceLocator();
        var plan = ModelFactory.readPlanFromYaml(resourceLocator.getInputStreamFactory(planLocation));
        var resourceLocationBuilder = new ResourceLocationBuilder(planLocation, "_", "yaml");
        var planMapper = ReferencePlanMapper.of(resourceLocationBuilder, resourceLocator);

        // when
        var newPlan = planMapper.map(plan);

        // then
        assertEquals(WrapperComponent.TYPE_NAME, newPlan.getComponents().get("wrapperComponentName").componentTypeName());

    }

    @Test
    void testReferenceMapperWithFailure() throws IOException, ModelFormatException {

        // given
        var planLocation = "src/test/resources/componentsPlanMapperTest/failedPlan.yaml";
        var resourceLocator = new FileResourceLocator();
        var plan = ModelFactory.readPlanFromYaml(resourceLocator.getInputStreamFactory(planLocation));
        var resourceLocationBuilder = new ResourceLocationBuilder(planLocation, "_", "yaml");
        var referenceComponentMapper = ReferenceComponentMapper.of(resourceLocationBuilder, resourceLocator);
        var planMapper = ReferencePlanMapper.of(resourceLocationBuilder, resourceLocator);

        // when
        assertThrows(PlanMapperException.class, () -> planMapper.map(plan));

    }
}