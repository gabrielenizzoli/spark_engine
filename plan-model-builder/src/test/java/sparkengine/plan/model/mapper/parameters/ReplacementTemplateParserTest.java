package sparkengine.plan.model.mapper.parameters;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReplacementTemplateParserTest {

    @Test
    void testReplaceTemplate() {

        // given
        var replaceTemplate = new ReplacementTemplateParser("${", "}");
        var params = Map.of("KEY1", "Kelly", "KEY4", "false");

        // when
        var outcome = replaceTemplate.replaceTemplate("hi there ${KEY1}, today is ${KEY3:awesome}!", params);

        // then
        assertEquals("hi there Kelly, today is awesome!", outcome);
    }

    @Test
    void testReplaceTemplate_missingParam() {

        // given
        var replaceTemplate = new ReplacementTemplateParser("${", "}");
        var params = Map.of("KEY1", "Kelly", "KEY4", "false");

        // then
        assertThrows(NullPointerException.class, () -> replaceTemplate.replaceTemplate("hi there ${KEY10}, today is ${KEY3:awesome}!", params));

    }

}