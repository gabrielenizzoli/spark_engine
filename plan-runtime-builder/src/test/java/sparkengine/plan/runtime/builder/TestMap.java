package sparkengine.plan.runtime.builder;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import sparkengine.spark.transformation.DataTransformation;

public class TestMap implements DataTransformation<Row, Row> {

    @Override
    public Dataset<Row> apply(Dataset<Row> dataset) {
        return dataset.selectExpr("value+1 as value").groupBy().sum("value");
    }

}
