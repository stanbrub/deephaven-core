//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.generator;

import gnu.trove.list.array.TIntArrayList;

public class FromUniqueIntArrayGenerator extends AbstractFromUniqueAdaptableGenerator<TIntArrayList, int[]> {
    public FromUniqueIntArrayGenerator(
            UniqueIntArrayGenerator uniqueStringArrayGenerator,
            IntArrayGenerator defaultGenerator,
            double existingFraction) {
        super(int[].class, uniqueStringArrayGenerator, defaultGenerator, TIntArrayList[]::new, existingFraction);
    }
}
