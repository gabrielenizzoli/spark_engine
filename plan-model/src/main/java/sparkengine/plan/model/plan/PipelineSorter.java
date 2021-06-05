package sparkengine.plan.model.plan;

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PipelineSorter {

    private static final Comparator<PipelineLayout> PIPELINE_LAYOUT_COMPARATOR = Comparator
            .comparing(PipelineLayout::getComponent)
            .thenComparing(PipelineLayout::getSink);

    private static final Comparator<Pipeline> PIPELINE_COMPARATOR = Comparator
            .comparingInt((Pipeline pipeline) -> Optional.ofNullable(pipeline.getOrder()).orElse(0))
            .thenComparing(Pipeline::getLayout, PIPELINE_LAYOUT_COMPARATOR);

    private static final Comparator<Map.Entry<String, Pipeline>> PIPELINE_ENTRY_COMPARATOR = Comparator
            .comparing((Function<Map.Entry<String, Pipeline>, Pipeline>) Map.Entry::getValue, PIPELINE_COMPARATOR)
            .thenComparing(Map.Entry::getKey);

    public static List<Map.Entry<String, Pipeline>> orderPipelines(@Nonnull Map<String, Pipeline> pipelines) {
        return pipelines.entrySet().stream()
                .sorted(PIPELINE_ENTRY_COMPARATOR)
                .collect(Collectors.toList());
    }

    public static int compare(PipelineLayout p1, PipelineLayout p2) {
        return PIPELINE_LAYOUT_COMPARATOR.compare(p1, p2);
    }

}
