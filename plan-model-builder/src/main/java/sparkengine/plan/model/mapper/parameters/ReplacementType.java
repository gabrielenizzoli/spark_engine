package sparkengine.plan.model.mapper.parameters;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

enum ReplacementType {
    STRING,
    NUMBER,
    BOOLEAN;

    @Nullable
    Object convert(String str) {
        if (str == null || str.isBlank()) {
            return null;
        }
        switch (this) {
            case STRING:
                return str;
            case BOOLEAN:
                return Boolean.parseBoolean(str);
            case NUMBER: {
                try {
                    var number = Long.parseLong(str);
                    if (number >= Integer.MIN_VALUE && number <= Integer.MAX_VALUE) {
                        return (int)number;
                    }
                    return number;
                } catch (NumberFormatException e) {
                    return Double.parseDouble(str);
                }
            }
            default:
                throw new IllegalArgumentException("unmanaged conversion type " + this);
        }
    }

}
