package org.example.gtfsynq.shared.persistence;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.gtfsynq.shared.protocol.offheap.OffHeapLongTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Saves and loads {@link OffHeapLongTable} state to/from a file
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OffHeapFileScribe {

    /**
     * The path to the file where state is saved/loaded.
     */
    private final Path savePath;

    @Autowired
    public OffHeapFileScribe(
        @Value("${state.save.path:state_dump.bin}") String path
    ) {
        this.savePath = Path.of(path);
        log.info(
            "State Scribe initialized with path: {}",
            this.savePath.toAbsolutePath()
        );
    }

    /**
     * Dumps the state of the given {@link OffHeapLongTable} to the save path
     *
     * @param table the table to dump
     */
    public void dump(OffHeapLongTable table) {
        var start = System.currentTimeMillis();
        try (
            var channel = FileChannel.open(
                savePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
            )
        ) {
            // Write the off-heap memory segment to the file
            channel.write(table.getSegment().asByteBuffer());

            log.info(
                "State dumped to {} in {}ms",
                savePath,
                System.currentTimeMillis() - start
            );
        } catch (IOException e) {
            log.error("Failed to dump state", e);
        }
    }

    /**
     * Loads the state of the given {@link OffHeapLongTable} from the save path
     *
     * @param table the table to load
     */
    public void load(OffHeapLongTable table) {
        if (!savePath.toFile().exists()) return;

        var start = System.currentTimeMillis();
        try (
            var channel = FileChannel.open(savePath, StandardOpenOption.READ)
        ) {
            // Read the file directly back into the off-heap memory segment
            channel.read(table.getSegment().asByteBuffer());

            log.info(
                "State loaded from {} in {}ms",
                savePath,
                System.currentTimeMillis() - start
            );
        } catch (IOException e) {
            log.error("Failed to load state", e);
        }
    }
}
