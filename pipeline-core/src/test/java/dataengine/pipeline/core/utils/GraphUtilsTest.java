package dataengine.pipeline.core.utils;

import dataengine.pipeline.core.source.GraphException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

class GraphUtilsTest {

    @Test
    public void testSimpleGraph() throws GraphException {

        Map<String, Set<String>> graph = new HashMap<>();
        graph.put("a", Collections.emptySet());

        Set<List<String>> paths = GraphUtils.pathsToSources(Collections.singleton("a"), graph);

        Assertions.assertEquals(
                Collections.singleton(Collections.singletonList("a")),
                paths
        );

    }

    @Test
    public void testMissingRoot() {

        Map<String, Set<String>> graph = new HashMap<>();
        graph.put("a", Collections.emptySet());

        Assertions.assertThrows(GraphException.MissingNode.class, () -> GraphUtils.pathsToSources(Collections.singleton("b"), graph));

    }

    @Test
    public void testGraphPaths() throws GraphException {

        Map<String, Set<String>> graph = new HashMap<>();
        graph.put("a", new HashSet<>(Arrays.asList("b", "c", "e")));
        graph.put("b", new HashSet<>(Arrays.asList("c", "d")));
        graph.put("c", Collections.emptySet());
        graph.put("d", Collections.emptySet());
        graph.put("e", new HashSet<>(Arrays.asList("b", "d")));

        Set<List<String>> paths = GraphUtils.pathsToSources(Collections.singleton("a"), graph);

        Assertions.assertEquals(
                new HashSet<>(Arrays.asList(
                        Arrays.asList("a", "b", "c"),
                        Arrays.asList("a", "b", "d"),
                        Arrays.asList("a", "c"),
                        Arrays.asList("a", "e", "b", "c"),
                        Arrays.asList("a", "e", "b", "d"),
                        Arrays.asList("a", "e", "d")
                )),
                paths
        );

    }

    @Test
    public void testMultiRootGraphPaths() throws GraphException {

        Map<String, Set<String>> graph = new HashMap<>();
        graph.put("a", new HashSet<>(Arrays.asList("b", "c", "e")));
        graph.put("b", new HashSet<>(Arrays.asList("c", "d")));
        graph.put("c", Collections.emptySet());
        graph.put("d", Collections.emptySet());
        graph.put("e", new HashSet<>(Arrays.asList("b", "d")));

        Set<List<String>> paths = GraphUtils.pathsToSources(new HashSet<>(Arrays.asList("b", "e")), graph);

        Assertions.assertEquals(
                new HashSet<>(Arrays.asList(
                        Arrays.asList("b", "c"),
                        Arrays.asList("b", "d"),
                        Arrays.asList("e", "b", "c"),
                        Arrays.asList("e", "b", "d"),
                        Arrays.asList("e", "d")
                )),
                paths
        );

    }

    @Test
    public void testMissingReference()  {

        Map<String, Set<String>> graph = new HashMap<>();
        graph.put("a", new HashSet<>(Arrays.asList("b", "c", "e")));
        graph.put("b", new HashSet<>(Arrays.asList("c", "d")));
        graph.put("d", Collections.emptySet());
        graph.put("e", new HashSet<>(Arrays.asList("b", "d")));

        Assertions.assertThrows(GraphException.MissingNode.class, () -> GraphUtils.pathsToSources(Collections.singleton("a"), graph));

    }

    @Test
    public void testCyclicalReference()  {

        Map<String, Set<String>> graph = new HashMap<>();
        graph.put("a", new HashSet<>(Arrays.asList("b", "c", "e")));
        graph.put("b", new HashSet<>(Arrays.asList("c", "d")));
        graph.put("c", new HashSet<>(Collections.singletonList("e")));
        graph.put("d", Collections.emptySet());
        graph.put("e", new HashSet<>(Arrays.asList("b", "d")));

        Assertions.assertThrows(GraphException.RepeatedNode.class, () -> GraphUtils.pathsToSources(Collections.singleton("a"), graph));

    }

}