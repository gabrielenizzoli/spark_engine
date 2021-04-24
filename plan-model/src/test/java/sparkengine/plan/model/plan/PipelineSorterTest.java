package sparkengine.plan.model.plan;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class PipelineSorterTest {

    @Test
    void comparePipelineLayout() {
        assertTrue(PipelineSorter.compare(new PipelineLayout("a", "a"), new PipelineLayout("a", "a")) == 0);

        assertTrue(PipelineSorter.compare(new PipelineLayout("a", "a"), new PipelineLayout("a", "b")) < 0);
        assertTrue(PipelineSorter.compare(new PipelineLayout("a", "b"), new PipelineLayout("a", "a")) > 0);

        assertTrue(PipelineSorter.compare(new PipelineLayout("a", "a"), new PipelineLayout("b", "a")) < 0);
        assertTrue(PipelineSorter.compare(new PipelineLayout("b", "a"), new PipelineLayout("a", "a")) > 0);
    }

    @Test
    void orderPipelinesSimpleCase() {

        var pipe1 = Pipeline.builder().withLayout(new PipelineLayout("b", "a")).build();
        var pipe2 = Pipeline.builder().withLayout(new PipelineLayout("a", "b")).build();
        var pipe3 = Pipeline.builder().withLayout(new PipelineLayout("a", "a")).build();

        var pipes = Map.of("pipe1", pipe1, "pipe2", pipe2, "pipe3", pipe3);

        assertEquals(
                List.of("pipe3", "pipe2", "pipe1"),
                PipelineSorter.orderPipelines(pipes).stream().map(Map.Entry::getKey).collect(Collectors.toList()));
    }

    @Test
    void orderPipelinesWithOrder() {

        var pipe1 = Pipeline.builder().withOrder(-1).withLayout(new PipelineLayout("b", "a")).build();
        var pipe2 = Pipeline.builder().withLayout(new PipelineLayout("a", "b")).build();
        var pipe3 = Pipeline.builder().withLayout(new PipelineLayout("a", "a")).build();

        var pipes = Map.of("pipe1", pipe1, "pipe2", pipe2, "pipe3", pipe3);

        assertEquals(
                List.of("pipe1", "pipe3", "pipe2"),
                PipelineSorter.orderPipelines(pipes).stream().map(Map.Entry::getKey).collect(Collectors.toList()));
    }

}