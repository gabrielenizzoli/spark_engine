package sparkengine.plan.app.runner;

import lombok.Value;
import sparkengine.plan.model.builder.input.AppResourceLocator;
import sparkengine.plan.model.builder.input.InputStreamFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.InputStream;
import java.util.Optional;

@Value(staticConstructor = "of")
public class PlanDefinition {

    @Nullable
    InputStream planInputStream;
    @Nonnull
    String planLocation;

    public static PlanDefinition planLocation(String planLocation) {
        return PlanDefinition.of(null, planLocation);
    }

    public InputStreamFactory getPlanInputStreamFactory() {
        return Optional
                .ofNullable(planInputStream)
                .map(is -> (InputStreamFactory)(() -> is))
                .orElse(new AppResourceLocator().getInputStreamFactory(planLocation));
    }

}
