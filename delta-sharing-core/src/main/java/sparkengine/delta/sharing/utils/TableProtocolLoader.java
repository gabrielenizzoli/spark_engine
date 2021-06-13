package sparkengine.delta.sharing.utils;

import com.google.common.hash.Hashing;
import io.delta.standalone.internal.date20210612.PartitionFilterUtils;
import lombok.Value;
import org.apache.spark.sql.delta.DeltaLog;
import scala.Tuple2;
import scala.collection.JavaConverters;
import sparkengine.delta.sharing.protocol.v1.*;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Value
public class TableProtocolLoader {

    @Nonnull
    DeltaLog deltaLog;

    public TableProtocolLoader update() {
        deltaLog.update(false);
        return this;
    }

    public List<Wrapper> loadTableProtocol() {
        return loadTableProtocol(true, List.of());
    }

    public List<Wrapper> loadTableProtocol(boolean includeFiles,
                                           List<String> predicateHits) {
        var snapshot = deltaLog.snapshot();

        var protocol = Protocol.builder().withMinReaderVersion(snapshot.protocol().minReaderVersion()).build();
        var metadata = Metadata.builder()
                .withId(snapshot.metadata().id())
                .withName(snapshot.metadata().name())
                .withDescription(snapshot.metadata().description())
                .withFormat(Format.builder().build())
                .withSchemaString(snapshot.metadata().schemaString())
                .withPartitionColumns(new ArrayList<>(JavaConverters.asJavaCollection(snapshot.metadata().partitionColumns())))
                .build();

        var wrappers = new ArrayList<Wrapper>();
        wrappers.add(Wrapper.builder().withProtocol(protocol).build());
        wrappers.add(Wrapper.builder().withMetaData(metadata).build());

        if (includeFiles) {
            var addFiles = snapshot.allFiles().collectAsList();
            if (snapshot.metadata().partitionColumns().nonEmpty()) {
                var newFiles = PartitionFilterUtils.evaluatePredicate(
                        snapshot.metadata().schemaString(),
                        snapshot.metadata().partitionColumns(),
                        JavaConverters.asScalaBuffer(predicateHits),
                        JavaConverters.asScalaBuffer(addFiles)
                );
                addFiles = JavaConverters.seqAsJavaList(newFiles);
            }

            var files = addFiles.stream()
                    .map(addFile -> File.builder()
                            .withId(Hashing.md5().hashString(addFile.path(), StandardCharsets.UTF_8).toString())
                            .withUrl(addFile.path())
                            .withSize(addFile.size())
                            .withPartitionValues(JavaConverters.asJavaCollection(addFile.partitionValues()).stream().collect(Collectors.toMap(Tuple2::_1, Tuple2::_2)))
                            .withStats(addFile.stats())
                            .build())
                    .collect(Collectors.toList());
            wrappers.addAll(files.stream().map(file -> Wrapper.builder().withFile(file).build()).collect(Collectors.toList()));
        }

        return wrappers;
    }

}
