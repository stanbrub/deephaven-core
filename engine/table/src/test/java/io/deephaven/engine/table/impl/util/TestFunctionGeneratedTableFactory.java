//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.QueryTableTest;
import io.deephaven.engine.testutil.ColumnInfo;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.testutil.EvalNuggetInterface;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.qst.type.Type;

import java.util.Random;

import static io.deephaven.engine.table.impl.util.TestKeyedArrayBackedInputTable.handleDelayedRefresh;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;

public class TestFunctionGeneratedTableFactory extends RefreshingTableTestCase {
    public void testIterative() {
        Random random = new Random(0);
        ColumnInfo<?, ?>[] columnInfo;
        int size = 50;
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new SetGenerator<>("a", "b", "c", "d", "e"),
                        new IntGenerator(10, 100),
                        new SetGenerator<>(10.1, 20.1, 30.1)));

        final Table functionBacked = FunctionGeneratedTableFactory.create(() -> queryTable, queryTable);

        assertTableEquals(queryTable, functionBacked, TableDiff.DiffItems.DoublesExact);

        EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new QueryTableTest.TableComparator(functionBacked, queryTable),
                // Note: disable update validation since the function backed table's prev values will always be
                // incorrect
                EvalNugget.from(() -> {
                    return ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                            () -> functionBacked.update("Mult=intCol * doubleCol"));
                }),
        };

        for (int i = 0; i < 75; i++) {
            simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }

    public void testNoSources() {
        // If no sources are specified, function should still run once on initialization.
        final Table functionBacked =
                FunctionGeneratedTableFactory.create(() -> newTable(
                        stringCol("StringCol", "MyString"),
                        intCol("IntCol", 12345)));

        assertEquals(functionBacked.size(), 1);
        assertTableEquals(newTable(
                stringCol("StringCol", "MyString"),
                intCol("IntCol", 12345)), functionBacked);
    }

    public void testMultipleSources() throws Exception {
        final AppendOnlyArrayBackedInputTable source1 = AppendOnlyArrayBackedInputTable.make(TableDefinition.of(
                ColumnDefinition.of("StringCol", Type.stringType())));
        final BaseArrayBackedInputTable.ArrayBackedInputTableUpdater inputTable1 = source1.makeUpdater();

        final AppendOnlyArrayBackedInputTable source2 = AppendOnlyArrayBackedInputTable.make(TableDefinition.of(
                ColumnDefinition.of("IntCol", Type.intType())));
        final BaseArrayBackedInputTable.ArrayBackedInputTableUpdater inputTable2 = source2.makeUpdater();

        final Table functionBacked =
                FunctionGeneratedTableFactory.create(() -> source1.lastBy().naturalJoin(source2, ""), source1, source2);

        assertEquals(functionBacked.size(), 0);

        handleDelayedRefresh(() -> {
            inputTable1.addAsync(newTable(stringCol("StringCol", "MyString")), t -> {
            });
            inputTable2.addAsync(newTable(intCol("IntCol", 12345)), t -> {
            });
        }, source1, source2);

        assertEquals(functionBacked.size(), 1);
        assertTableEquals(newTable(
                stringCol("StringCol", "MyString"),
                intCol("IntCol", 12345)), functionBacked);
    }
}
