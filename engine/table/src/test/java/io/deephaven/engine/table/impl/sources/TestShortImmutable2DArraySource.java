//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TestCharacterImmutable2DArraySource and run "./gradlew replicateSourceAndChunkTests" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.impl.sources.immutable.Immutable2DShortArraySource;
import org.jetbrains.annotations.NotNull;

public class TestShortImmutable2DArraySource extends AbstractShortColumnSourceTest {
    @Override
    int getSourceSize() {
        return 1 << 16;
    }

    @NotNull
    @Override
    Immutable2DShortArraySource makeTestSource() {
        final int capacity = getSourceSize();
        final Immutable2DShortArraySource flatShortArraySource = new Immutable2DShortArraySource(12);
        flatShortArraySource.ensureCapacity(capacity);
        try (final ChunkSink.FillFromContext ffc = flatShortArraySource.makeFillFromContext(capacity);
                final WritableShortChunk nullChunk = WritableShortChunk.makeWritableChunk(capacity)) {
            nullChunk.fillWithNullValue(0, capacity);
            flatShortArraySource.fillFromChunk(ffc, nullChunk, RowSetFactory.flat(capacity));
        }
        return flatShortArraySource;
    }
}
