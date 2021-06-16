package sparkengine.delta.sharing.utils;

import com.google.common.hash.Hashing;
import io.delta.standalone.internal.date20210612.PartitionFilterUtils;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.delta.DeltaLog;
import scala.Tuple2;
import scala.collection.JavaConverters;
import sparkengine.delta.sharing.protocol.v1.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@AllArgsConstructor
public class TableLayout {

    @Nonnull
    DeltaLog deltaLog;

    public TableLayout update() {
        deltaLog.update(false);
        return this;
    }

    public Stream<Wrapper> streamTableMetadataProtocol() {
        return streamTableProtocol(false, null);
    }

    public Stream<Wrapper> streamTableFilesProtocol() {
        return streamTableProtocol(true, null);
    }

    public long getTableVersion() {
        return deltaLog.snapshot().version();
    }

    public Stream<Wrapper> streamTableProtocol(boolean includeFiles,
                                               @Nullable List<String> predicateHits) {
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

        var wrappers = Stream.of(
                Wrapper.builder().withProtocol(protocol).build(),
                Wrapper.builder().withMetaData(metadata).build());

        if (includeFiles) {
            var addFiles = snapshot.allFiles().collectAsList();
            if (snapshot.metadata().partitionColumns().nonEmpty()) {
                var newFiles = PartitionFilterUtils.evaluatePredicate(
                        snapshot.metadata().schemaString(),
                        snapshot.metadata().partitionColumns(),
                        JavaConverters.asScalaBuffer(Optional.ofNullable(predicateHits).orElse(List.of())),
                        JavaConverters.asScalaBuffer(addFiles)
                );
                addFiles = JavaConverters.seqAsJavaList(newFiles);
            }

            var files = addFiles.stream()
                    .map(addFile -> File.builder()
                            .withId(Hashing.md5().hashString(addFile.path(), StandardCharsets.UTF_8).toString())
                            .withUrl(absolutePath(deltaLog.dataPath(), addFile.path()).toString())
                            .withSize(addFile.size())
                            .withPartitionValues(JavaConverters.asJavaCollection(addFile.partitionValues()).stream().collect(Collectors.toMap(Tuple2::_1, Tuple2::_2)))
                            .withStats(addFile.stats())
                            .build())
                    .map(file -> Wrapper.builder().withFile(file).build());

            wrappers = Stream.concat(wrappers, files);
        }

        return wrappers;
    }

    @SneakyThrows
    private Path absolutePath(Path path, String child) {
        var p = new Path(new URI(child));
        if (p.isAbsolute()) {
            throw new IllegalStateException("table containing absolute paths cannot be shared");
        }
        return new Path(path, p);
    }

}
