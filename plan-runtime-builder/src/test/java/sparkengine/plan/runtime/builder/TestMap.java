package sparkengine.plan.runtime.builder;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import sparkengine.spark.transformation.DataTransformation;
import sparkengine.spark.transformation.DataTransformationN;

import java.util.List;

public class TestMap implements DataTransformation<Row, Row> {

    @Override
    public Dataset<Row> apply(Dataset<Row> dataset) {
        return dataset.selectExpr("value+1 as value").groupBy().sum("value");
    }

}
