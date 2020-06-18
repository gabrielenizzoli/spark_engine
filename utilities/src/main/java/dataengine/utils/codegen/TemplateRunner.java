package dataengine.utils.codegen;

import com.samskivert.mustache.Mustache;
import lombok.Builder;
import lombok.Cleanup;
import lombok.Value;

import java.io.*;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Value
@Builder
public class TemplateRunner {

    String name;
    int start;
    int end;

    public void run() throws IOException {
        final File templateDir = new File("codeGenerationTemplates/src/main/resources");
        final File outDir = new File("codeGenerationTemplates/target");
        @Cleanup final Writer out = new FileWriter(new File(outDir, name + ".txt"));

        Mustache.Compiler c = Mustache.compiler().withLoader(n -> new FileReader(new File(templateDir, n + ".mustache")));

        c.loadTemplate(name).execute(new Object() {
            List<Cardinality> merges = IntStream.range(start, end).mapToObj(Cardinality::of).collect(Collectors.toList());
        }, out);
    }

    @Value
    @Builder
    public static class Cardinality {
        int number;
        List<Integer> i;

        public static Cardinality of(int n) {
            return Cardinality.builder()
                    .number(n)
                    .i(IntStream.range(1, n + 1).boxed().collect(Collectors.toList()))
                    .build();
        }
    }

}
