package dataengine.utils.codegen;

import java.io.IOException;

public class SqlMergeTemplateApp {

    public static void main(String[] args) throws IOException {

        String name = "SqlMerge";
        int start = 3;
        int end = 11;

        TemplateRunner.builder().name(name).start(start).end(end).build().run();

    }

}
