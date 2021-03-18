package sparkengine.spark.sql.udf;

import sparkengine.spark.test.BeanUserDefinedType;
import sparkengine.spark.test.WrapperUserDefinedType;

public class TestUserDefinedType extends WrapperUserDefinedType<TestBean> {

    public TestUserDefinedType() {
        super(BeanUserDefinedType.getUDT(TestBean.class));
    }

}
