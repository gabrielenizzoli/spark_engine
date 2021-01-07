package dataengine.pipeline.core.supplier.impl;

import dataengine.pipeline.core.supplier.DatasetSupplier;
import dataengine.spark.test.SparkSessionBase;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class InlineObjectSupplierTest extends SparkSessionBase {

    @Test
    void testIntegers() {
        var ds = InlineObjectSupplier.<Integer>builder().objects(List.of(1,2,3,4)).encoder(Encoders.INT()).build().get();
        Assertions.assertEquals(List.of(1,2,3,4), ds.collectAsList());
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor(staticName = "of")
    public static class TestObj {
        int value;
        String str;
    }

    @Test
    void testObjects() {
        var ds = InlineObjectSupplier.<TestObj>builder()
                .objects(List.of(TestObj.of(1, "one"), TestObj.of(2, "two"), TestObj.of(3, "three")))
                .encoder(Encoders.bean(TestObj.class)).build()
                .get()
                .filter((FilterFunction<TestObj>)  t -> t.getValue() == 1);
        Assertions.assertEquals(List.of(TestObj.of(1, "one")), ds.collectAsList());
    }

}
