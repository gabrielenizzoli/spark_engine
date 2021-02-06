package sparkengine.plan.model.mapper.sql;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum ResolverMode {
    SKIP(false, false),
    VALIDATE(true, false),
    INFER(false, true);

    boolean validate;
    boolean replace;
}
