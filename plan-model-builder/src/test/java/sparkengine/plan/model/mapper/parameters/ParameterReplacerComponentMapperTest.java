package sparkengine.plan.model.mapper.parameters;

import org.junit.jupiter.api.Test;
import sparkengine.plan.model.component.impl.InlineComponent;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ParameterReplacerComponentMapperTest {

    @Test
    void testInlineComponent() throws Exception {

        // given
        var params = Map.of("KEY1", "1", "KEY3", "text here", "KEY4", "false");
        var mapper = ParameterReplacerComponentMapper.of(params, "${", "}");
        var component = InlineComponent.builder().withData(List.of(Map
                .of("column1", "${n:KEY1:100}",
                        "column2", "${n:KEY2:200}",
                        "column3", "${KEY3}",
                        "column4", "${b:KEY4}"))).build();

        // when
        var outComponent = mapper.mapInlineComponent(null, component);

        // then
        assertTrue(outComponent instanceof InlineComponent);
        var inlineComponent = (InlineComponent) outComponent;
        assertEquals(1, inlineComponent.getData().size());
        assertEquals(
                Map.of("column1", 1, "column2", 200, "column3", "text here", "column4", false),
                inlineComponent.getData().get(0));

    }

    @Test
    void testInlineComponent_missingParam() throws Exception {

        // given
        var params = Map.of("KEY1", "1");
        var mapper = ParameterReplacerComponentMapper.of(params, "${", "}");
        var component = InlineComponent.builder().withData(List.of(Map
                .of("column1", "${n:KEY1:100}", "column2", "${n:KEY2}"))).build();

        // when
        assertThrows(NullPointerException.class, () -> mapper.mapInlineComponent(null, component));

    }

}