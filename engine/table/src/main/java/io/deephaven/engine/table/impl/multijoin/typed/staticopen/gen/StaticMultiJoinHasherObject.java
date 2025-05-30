//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run ReplicateTypedHashers or ./gradlew replicateTypedHashers to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.multijoin.typed.staticopen.gen;

import static io.deephaven.util.compare.ObjectComparisons.eq;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.ObjectChunkHasher;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.multijoin.StaticMultiJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableObjectArraySource;
import java.lang.Object;
import java.lang.Override;
import java.util.Arrays;

final class StaticMultiJoinHasherObject extends StaticMultiJoinStateManagerTypedBase {
    private final ImmutableObjectArraySource mainKeySource0;

    public StaticMultiJoinHasherObject(ColumnSource[] tableKeySources,
            ColumnSource[] originalTableKeySources, int tableSize, double maximumLoadFactor,
            double targetLoadFactor) {
        super(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor);
        this.mainKeySource0 = (ImmutableObjectArraySource) super.mainKeySources[0];
        this.mainKeySource0.ensureCapacity(tableSize);
    }

    private int nextTableLocation(int tableLocation) {
        return (tableLocation + 1) & (tableSize - 1);
    }

    protected void buildFromTable(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            LongArraySource tableRedirSource, long tableNumber) {
        final ObjectChunk<Object, Values> keyChunk0 = sourceKeyChunks[0].asObjectChunk();
        final int chunkSize = keyChunk0.size();
        final LongChunk<OrderedRowKeys> rowKeyChunk = rowSequence.asRowKeyChunk();
        for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition) {
            final Object k0 = keyChunk0.get(chunkPosition);
            final int hash = hash(k0);
            final int firstTableLocation = hashToTableLocation(hash);
            int tableLocation = firstTableLocation;
            while (true) {
                int slotValue = slotToOutputRow.getUnsafe(tableLocation);
                if (isStateEmpty(slotValue)) {
                    numEntries++;
                    mainKeySource0.set(tableLocation, k0);
                    final int outputKey = numEntries - 1;
                    slotToOutputRow.set(tableLocation, outputKey);
                    tableRedirSource.set(outputKey, rowKeyChunk.get(chunkPosition));
                    outputKeySources[0].set((long)outputKey, k0);
                    break;
                } else if (eq(mainKeySource0.getUnsafe(tableLocation), k0)) {
                    if (tableRedirSource.getLong(slotValue) != NO_REDIRECTION) {
                        throw new IllegalStateException("Duplicate key found for " + keyString(sourceKeyChunks, chunkPosition) + " in table " + tableNumber + ".");
                    }
                    tableRedirSource.set(slotValue, rowKeyChunk.get(chunkPosition));
                    break;
                } else {
                    tableLocation = nextTableLocation(tableLocation);
                    Assert.neq(tableLocation, "tableLocation", firstTableLocation, "firstTableLocation");
                }
            }
        }
    }

    private static int hash(Object k0) {
        int hash = ObjectChunkHasher.hashInitialSingle(k0);
        return hash;
    }

    private static boolean isStateEmpty(int state) {
        return state == EMPTY_OUTPUT_ROW;
    }

    @Override
    protected void rehashInternalFull(final int oldSize) {
        final Object[] destKeyArray0 = new Object[tableSize];
        final int[] destState = new int[tableSize];
        Arrays.fill(destState, EMPTY_OUTPUT_ROW);
        final Object [] originalKeyArray0 = mainKeySource0.getArray();
        mainKeySource0.setArray(destKeyArray0);
        final int [] originalStateArray = slotToOutputRow.getArray();
        slotToOutputRow.setArray(destState);
        for (int sourceBucket = 0; sourceBucket < oldSize; ++sourceBucket) {
            final int currentStateValue = originalStateArray[sourceBucket];
            if (isStateEmpty(currentStateValue)) {
                continue;
            }
            final Object k0 = originalKeyArray0[sourceBucket];
            final int hash = hash(k0);
            final int firstDestinationTableLocation = hashToTableLocation(hash);
            int destinationTableLocation = firstDestinationTableLocation;
            while (true) {
                if (isStateEmpty(destState[destinationTableLocation])) {
                    destKeyArray0[destinationTableLocation] = k0;
                    destState[destinationTableLocation] = originalStateArray[sourceBucket];
                    break;
                }
                destinationTableLocation = nextTableLocation(destinationTableLocation);
                Assert.neq(destinationTableLocation, "destinationTableLocation", firstDestinationTableLocation, "firstDestinationTableLocation");
            }
        }
    }
}
