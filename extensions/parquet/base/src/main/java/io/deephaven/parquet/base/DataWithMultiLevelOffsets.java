//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import java.nio.IntBuffer;

public class DataWithMultiLevelOffsets {
    public final IntBuffer[] offsets;
    public final Object values;

    DataWithMultiLevelOffsets(IntBuffer[] offsets, Object values) {
        this.offsets = offsets;
        this.values = values;
    }
}
