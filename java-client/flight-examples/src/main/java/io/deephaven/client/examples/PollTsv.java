//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableHandleManager;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TimeTable;
import org.apache.arrow.flight.FlightStream;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.time.Duration;

@Command(name = "poll-tsv", mixinStandardHelpOptions = true,
        description = "Send a QST, poll the results, and convert to TSV", version = "0.1.0")
class PollTsv extends FlightExampleBase {

    static class Mode {
        @Option(names = {"-b", "--batch"}, required = true, description = "Batch mode")
        boolean batch;

        @Option(names = {"-s", "--serial"}, required = true, description = "Serial mode")
        boolean serial;
    }

    @ArgGroup(exclusive = true)
    Mode mode;

    @Option(names = {"-i", "--interval"}, description = "The interval.", defaultValue = "PT1s")
    Duration interval;

    @Option(names = {"-c", "--count"}, description = "The number of polls.")
    Long count;

    TableSpec table = TimeTable.of(Duration.ofSeconds(1));

    @Override
    protected void execute(FlightSession flight) throws Exception {

        final TableHandleManager manager = mode == null ? flight.session()
                : mode.batch ? flight.session().batch() : flight.session().serial();

        long times = count == null ? Long.MAX_VALUE : count;

        try (final TableHandle handle = manager.execute(table)) {
            for (long i = 0; i < times; ++i) {
                long start = System.nanoTime();
                try (final FlightStream stream = flight.stream(handle)) {
                    if (i == 0) {
                        System.out.println(stream.getSchema());
                        System.out.println();
                    }
                    while (stream.next()) {
                        System.out.println(stream.getRoot().contentToTSVString());
                    }
                    long end = System.nanoTime();
                    System.out.printf("%s duration%n%n", Duration.ofNanos(end - start));
                    if (i + 1 < times) {
                        Thread.sleep(interval.toMillis());
                    }
                }
            }
        }
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new PollTsv()).execute(args);
        System.exit(execute);
    }
}
