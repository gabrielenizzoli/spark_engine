package sparkengine.plan.app.runner;

import lombok.Value;
import sparkengine.plan.model.builder.input.AppResourceLocator;
import sparkengine.plan.model.builder.input.InputStreamFactory;
import sparkengine.plan.model.plan.Plan;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.InputStream;
import java.util.Optional;

@Value(staticConstructor = "of")
public class PlanInfo {

    @Nullable
    InputStream planInputStream;
    @Nonnull
    String planLocation;

    public static PlanInfo planLocation(String planLocation) {
        return PlanInfo.of(null, planLocation);
    }

    public InputStreamFactory getPlanInputStreamFactory() {
        return Optional
                .ofNullable(planInputStream)
                .map(is -> (InputStreamFactory)(() -> is))
                .orElse(new AppResourceLocator().getInputStreamFactory(planLocation));
    }

}
