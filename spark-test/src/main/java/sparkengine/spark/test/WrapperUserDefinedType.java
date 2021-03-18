package sparkengine.spark.test;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.UDTRegistration;
import org.apache.spark.sql.types.UserDefinedType;

public abstract class WrapperUserDefinedType<T> extends UserDefinedType<T> {

    private UserDefinedType<T> udt;

    public WrapperUserDefinedType(UserDefinedType<T> udt) {
        this.udt = udt;
    }

    @Override
    public DataType sqlType() {
        return udt.sqlType();
    }

    @Override
    public Object serialize(T obj) {
        return udt.serialize(obj);
    }

    @Override
    public T deserialize(Object datum) {
        return udt.deserialize(datum);
    }

    @Override
    public Class<T> userClass() {
        return udt.userClass();
    }

    public void register() {
        UDTRegistration.register(userClass().getName(), this.getClass().getName());
    }

}
