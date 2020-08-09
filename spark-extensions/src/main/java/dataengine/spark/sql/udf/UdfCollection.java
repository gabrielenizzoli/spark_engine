package dataengine.spark.sql.udf;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@FunctionalInterface
public interface UdfCollection {

    List<Udf> getUdfs();

    static UdfCollection of(Udf... udfs) {
        return () -> Arrays.stream(udfs).filter(Objects::nonNull).collect(Collectors.toList());
    }

}
