package sparkengine.plan.model.mapper.parameters;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ReplacementKeyParserTest {

    public static final ReplacementKeyParser KEY_PARSER = new ReplacementKeyParser("${", "}");

    @Test
    void testReplacementKeys() {

        assertEquals(new ReplacementKey("TEST", ReplacementType.STRING, "hi"), KEY_PARSER.getReplacementKey("${s:TEST:hi}"));
        assertEquals(new ReplacementKey("TEST", ReplacementType.STRING, null), KEY_PARSER.getReplacementKey("${s:TEST}"));
        assertEquals(new ReplacementKey("TEST", ReplacementType.STRING, null), KEY_PARSER.getReplacementKey("${s:TEST:}"));
        assertEquals(new ReplacementKey("TEST", ReplacementType.STRING, null), KEY_PARSER.getReplacementKey("${TEST}"));

        assertEquals(new ReplacementKey("TEST", ReplacementType.BOOLEAN, false), KEY_PARSER.getReplacementKey("${b:TEST:false}"));
        assertEquals(new ReplacementKey("TEST", ReplacementType.BOOLEAN, true), KEY_PARSER.getReplacementKey("${b:TEST:true}"));

        assertEquals(new ReplacementKey("TEST", ReplacementType.NUMBER, 1.1), KEY_PARSER.getReplacementKey("${n:TEST:1.1}"));
        assertEquals(new ReplacementKey("TEST", ReplacementType.NUMBER, 100), KEY_PARSER.getReplacementKey("${n:TEST:100}"));
        assertEquals(new ReplacementKey("TEST", ReplacementType.NUMBER, null), KEY_PARSER.getReplacementKey("${n:TEST}"));

    }
}