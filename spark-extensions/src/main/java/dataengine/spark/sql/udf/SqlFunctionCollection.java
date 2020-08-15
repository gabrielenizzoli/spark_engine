package dataengine.spark.sql.udf;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@FunctionalInterface
public interface SqlFunctionCollection {

    List<SqlFunction> getSqlFunctions();

    static SqlFunctionCollection of(SqlFunction... sqlFunctions) {
        return () -> Arrays.stream(sqlFunctions).filter(Objects::nonNull).collect(Collectors.toList());
    }

}
