//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.api.util.NameValidator;
import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.ColumnToCodecMappings;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.hash.KeyedObjectKey.Basic;
import io.deephaven.parquet.base.ParquetUtils;
import io.deephaven.parquet.table.location.ParquetColumnResolver;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * This class provides instructions intended for read and write parquet operations (which take it as an optional
 * argument) specifying desired transformations. Examples are mapping column names and use of specific codecs during
 * (de)serialization.
 */
public abstract class ParquetInstructions implements ColumnToCodecMappings {

    public static final String DEFAULT_COMPRESSION_CODEC_NAME = CompressionCodecName.SNAPPY.toString();
    public static final int DEFAULT_MAXIMUM_DICTIONARY_KEYS = 1 << 20;
    public static final int DEFAULT_MAXIMUM_DICTIONARY_SIZE = 1 << 20;

    public static final int MIN_TARGET_PAGE_SIZE = Configuration.getInstance().getIntegerWithDefault(
            "Parquet.minTargetPageSize", 1 << 11); // 2KB

    public static final int DEFAULT_TARGET_PAGE_SIZE = Configuration.getInstance().getIntegerWithDefault(
            "Parquet.defaultTargetPageSize", 1 << 16); // 64KB

    /**
     * Throws an exception if {@link ParquetInstructions#getTableDefinition()} is empty.
     *
     * @param parquetInstructions the parquet instructions
     * @throws IllegalArgumentException if there is not a table definition
     */
    static TableDefinition ensureDefinition(final ParquetInstructions parquetInstructions) {
        return parquetInstructions.getTableDefinition()
                .orElseThrow(() -> new IllegalArgumentException("Table definition must be provided"));
    }

    private static final boolean DEFAULT_IS_REFRESHING = false;

    public interface OnWriteCompleted {
        void onWriteCompleted(CompletedParquetWrite completedParquetWrite);
    }

    public enum ParquetFileLayout {
        /**
         * A single parquet file.
         */
        SINGLE_FILE,

        /**
         * A single directory of parquet files.
         */
        FLAT_PARTITIONED,

        /**
         * A key-value directory partitioning of parquet files.
         */
        KV_PARTITIONED,

        /**
         * Layout can be used to describe:
         * <ul>
         * <li>A directory containing a {@value ParquetUtils#METADATA_FILE_NAME} parquet file and an optional
         * {@value ParquetUtils#COMMON_METADATA_FILE_NAME} parquet file
         * <li>A single parquet {@value ParquetUtils#METADATA_FILE_NAME} file
         * <li>A single parquet {@value ParquetUtils#COMMON_METADATA_FILE_NAME} file
         * </ul>
         */
        METADATA_PARTITIONED
    }

    private static final boolean DEFAULT_GENERATE_METADATA_FILES = false;

    static final String UUID_TOKEN = "{uuid}";
    static final String PARTITIONS_TOKEN = "{partitions}";
    static final String FILE_INDEX_TOKEN = "{i}";
    private static final String DEFAULT_BASE_NAME_FOR_PARTITIONED_PARQUET_DATA = UUID_TOKEN;

    private ParquetInstructions() {}

    public final String getColumnNameFromParquetColumnNameOrDefault(final String parquetColumnName) {
        final String mapped = getColumnNameFromParquetColumnName(parquetColumnName);
        return (mapped != null) ? mapped : parquetColumnName;
    }

    public abstract String getParquetColumnNameFromColumnNameOrDefault(final String columnName);

    public abstract String getColumnNameFromParquetColumnName(final String parquetColumnName);

    @Override
    public abstract String getCodecName(final String columnName);

    @Override
    public abstract String getCodecArgs(final String columnName);

    /**
     * @return A hint that the writer should use dictionary-based encoding for writing this column; never evaluated for
     *         non-String columns, defaults to false
     */
    public abstract boolean useDictionary(String columnName);

    /**
     * The field ID for the given {@code columnName}.
     *
     * @param columnName the Deephaven column name
     * @return the field id
     */
    public abstract OptionalInt getFieldId(final String columnName);

    public abstract Object getSpecialInstructions();

    public abstract String getCompressionCodecName();

    /**
     * @return The maximum number of unique keys the writer should add to a dictionary page before switching to
     *         non-dictionary encoding; never evaluated for non-String columns, ignored if
     *         {@link #useDictionary(String)}
     */
    public abstract int getMaximumDictionaryKeys();

    /**
     * @return The maximum number of bytes the writer should add to a dictionary before switching to non-dictionary
     *         encoding; never evaluated for non-String columns, ignored if {@link #useDictionary(String)}
     */
    public abstract int getMaximumDictionarySize();

    public abstract boolean isLegacyParquet();

    public abstract int getTargetPageSize();

    /**
     * @return if the data source is refreshing
     */
    public abstract boolean isRefreshing();

    /**
     * @return should we generate {@value ParquetUtils#METADATA_FILE_NAME} and
     *         {@value ParquetUtils#COMMON_METADATA_FILE_NAME} files while writing parquet files?
     */
    public abstract boolean generateMetadataFiles();

    public abstract Optional<ParquetFileLayout> getFileLayout();

    public abstract Optional<TableDefinition> getTableDefinition();

    public abstract Optional<Collection<List<String>>> getIndexColumns();

    public abstract Optional<ParquetColumnResolver.Factory> getColumnResolverFactory();

    /**
     * Creates a new {@link ParquetInstructions} object with the same properties as the current object but definition
     * set as the provided {@link TableDefinition}.
     */
    public abstract ParquetInstructions withTableDefinition(final TableDefinition tableDefinition);

    /**
     * Creates a new {@link ParquetInstructions} object with the same properties as the current object but layout set as
     * the provided {@link ParquetFileLayout}.
     */
    public abstract ParquetInstructions withLayout(final ParquetFileLayout fileLayout);

    /**
     * Creates a new {@link ParquetInstructions} object with the same properties as the current object but definition
     * and layout set as the provided values.
     */
    public abstract ParquetInstructions withTableDefinitionAndLayout(final TableDefinition tableDefinition,
            final ParquetFileLayout fileLayout);

    /**
     * Creates a new {@link ParquetInstructions} object with the same properties as the current object but index columns
     * set as the provided values.
     */
    @VisibleForTesting
    abstract ParquetInstructions withIndexColumns(final Collection<List<String>> indexColumns);

    /**
     * @return the base name for partitioned parquet data. Check
     *         {@link Builder#setBaseNameForPartitionedParquetData(String) setBaseNameForPartitionedParquetData} for
     *         more details about different tokens that can be used in the base name.
     */
    public abstract String baseNameForPartitionedParquetData();

    /**
     * @return A callback to be executed when on completing each parquet data file write (excluding the index and
     *         metadata files). This callback gets invoked by the writing thread in a linear fashion.
     */
    public abstract Optional<OnWriteCompleted> onWriteCompleted();

    public abstract Optional<SeekableChannelsProvider> getSeekableChannelsProviderForWriting();

    @VisibleForTesting
    public static boolean sameColumnNamesAndCodecMappings(final ParquetInstructions i1, final ParquetInstructions i2) {
        if (i1 == EMPTY) {
            if (i2 == EMPTY) {
                return true;
            }
            return ((ReadOnly) i2).columnNameToInstructions.isEmpty();
        }
        if (i2 == EMPTY) {
            return ((ReadOnly) i1).columnNameToInstructions.isEmpty();
        }
        return ReadOnly.sameCodecMappings((ReadOnly) i1, (ReadOnly) i2);
    }

    public static final ParquetInstructions EMPTY = new ParquetInstructions() {

        @Override
        public String getParquetColumnNameFromColumnNameOrDefault(final String columnName) {
            return columnName;
        }

        @Override
        @Nullable
        public String getColumnNameFromParquetColumnName(final String parquetColumnName) {
            return null;
        }

        @Override
        @Nullable
        public String getCodecName(final String columnName) {
            return null;
        }

        @Override
        @Nullable
        public String getCodecArgs(final String columnName) {
            return null;
        }

        @Override
        public boolean useDictionary(final String columnName) {
            return false;
        }

        @Override
        public OptionalInt getFieldId(String columnName) {
            return OptionalInt.empty();
        }

        @Override
        @Nullable
        public Object getSpecialInstructions() {
            return null;
        }

        @Override
        public String getCompressionCodecName() {
            return DEFAULT_COMPRESSION_CODEC_NAME;
        }

        @Override
        public int getMaximumDictionaryKeys() {
            return DEFAULT_MAXIMUM_DICTIONARY_KEYS;
        }

        @Override
        public int getMaximumDictionarySize() {
            return DEFAULT_MAXIMUM_DICTIONARY_SIZE;
        }

        @Override
        public boolean isLegacyParquet() {
            return false;
        }

        @Override
        public int getTargetPageSize() {
            return DEFAULT_TARGET_PAGE_SIZE;
        }

        @Override
        public boolean isRefreshing() {
            return DEFAULT_IS_REFRESHING;
        }

        @Override
        public boolean generateMetadataFiles() {
            return DEFAULT_GENERATE_METADATA_FILES;
        }

        @Override
        public String baseNameForPartitionedParquetData() {
            return DEFAULT_BASE_NAME_FOR_PARTITIONED_PARQUET_DATA;
        }

        @Override
        public Optional<ParquetFileLayout> getFileLayout() {
            return Optional.empty();
        }

        @Override
        public Optional<TableDefinition> getTableDefinition() {
            return Optional.empty();
        }

        @Override
        public Optional<Collection<List<String>>> getIndexColumns() {
            return Optional.empty();
        }

        @Override
        public Optional<ParquetColumnResolver.Factory> getColumnResolverFactory() {
            return Optional.empty();
        }

        @Override
        public @NotNull Optional<SeekableChannelsProvider> getSeekableChannelsProviderForWriting() {
            return Optional.empty();
        }

        @Override
        public ParquetInstructions withTableDefinition(@Nullable final TableDefinition useDefinition) {
            return withTableDefinitionAndLayout(useDefinition, null);
        }

        @Override
        public ParquetInstructions withLayout(@Nullable final ParquetFileLayout useLayout) {
            return withTableDefinitionAndLayout(null, useLayout);
        }

        @Override
        public ParquetInstructions withTableDefinitionAndLayout(
                @Nullable final TableDefinition useDefinition,
                @Nullable final ParquetFileLayout useLayout) {
            return new ReadOnly(null, null, getCompressionCodecName(), getMaximumDictionaryKeys(),
                    getMaximumDictionarySize(), isLegacyParquet(), getTargetPageSize(), isRefreshing(),
                    getSpecialInstructions(), generateMetadataFiles(), baseNameForPartitionedParquetData(),
                    useLayout, useDefinition, null, null, null, null);
        }

        @Override
        ParquetInstructions withIndexColumns(final Collection<List<String>> indexColumns) {
            return new ReadOnly(null, null, getCompressionCodecName(), getMaximumDictionaryKeys(),
                    getMaximumDictionarySize(), isLegacyParquet(), getTargetPageSize(), isRefreshing(),
                    getSpecialInstructions(), generateMetadataFiles(), baseNameForPartitionedParquetData(),
                    null, null, indexColumns, null, null, null);
        }

        @Override
        public Optional<OnWriteCompleted> onWriteCompleted() {
            return Optional.empty();
        }
    };

    private static class ColumnInstructions {

        private static final KeyedObjectKey<String, ColumnInstructions> COLUMN_NAME_KEY = new Basic<>() {
            @Override
            public String getKey(@NotNull final ColumnInstructions columnInstructions) {
                return columnInstructions.getColumnName();
            }
        };

        private static final KeyedObjectKey<String, ColumnInstructions> PARQUET_COLUMN_NAME_KEY = new Basic<>() {
            @Override
            public String getKey(@NotNull final ColumnInstructions columnInstructions) {
                return columnInstructions.getParquetColumnName();
            }
        };

        private final String columnName;
        private String parquetColumnName;
        private String codecName;
        private String codecArgs;
        private boolean useDictionary;
        private Integer fieldId;

        public ColumnInstructions(final String columnName) {
            this.columnName = Objects.requireNonNull(columnName);
            NameValidator.validateColumnName(columnName);
        }

        public String getColumnName() {
            return columnName;
        }

        public String getParquetColumnName() {
            return parquetColumnName != null ? parquetColumnName : columnName;
        }

        public ColumnInstructions setParquetColumnName(final String parquetColumnName) {
            if (this.parquetColumnName != null && !this.parquetColumnName.equals(parquetColumnName)) {
                throw new IllegalArgumentException(
                        "Cannot add a mapping from parquetColumnName=" + parquetColumnName
                                + ": columnName=" + columnName + " already mapped to parquetColumnName="
                                + this.parquetColumnName);
            }
            this.parquetColumnName = parquetColumnName;
            return this;
        }

        public String getCodecName() {
            return codecName;
        }

        public ColumnInstructions setCodecName(final String codecName) {
            this.codecName = codecName;
            return this;
        }

        public String getCodecArgs() {
            return codecArgs;
        }

        public ColumnInstructions setCodecArgs(final String codecArgs) {
            this.codecArgs = codecArgs;
            return this;
        }

        public boolean useDictionary() {
            return useDictionary;
        }

        public void useDictionary(final boolean useDictionary) {
            this.useDictionary = useDictionary;
        }

        public OptionalInt fieldId() {
            return fieldId == null ? OptionalInt.empty() : OptionalInt.of(fieldId);
        }

        public void setFieldId(final int fieldId) {
            if (this.fieldId != null && this.fieldId != fieldId) {
                throw new IllegalArgumentException(
                        String.format("Inconsistent fieldId for columnName=%s, already set fieldId=%d", columnName,
                                this.fieldId));
            }
            this.fieldId = fieldId;
        }
    }

    private static final class ReadOnly extends ParquetInstructions {
        private final KeyedObjectHashMap<String, ColumnInstructions> columnNameToInstructions;
        /**
         * Note parquetColumnNameToInstructions may be null while columnNameToInstructions is not null; We only store
         * entries in parquetColumnNameToInstructions when the parquetColumnName is different than the columnName (ie,
         * the column name mapping is not the default mapping)
         */
        private final KeyedObjectHashMap<String, ColumnInstructions> parquetColumnNameToInstructions;
        private final String compressionCodecName;
        final private int maximumDictionaryKeys;
        final private int maximumDictionarySize;
        private final boolean isLegacyParquet;
        private final int targetPageSize;
        private final boolean isRefreshing;
        private final Object specialInstructions;
        private final boolean generateMetadataFiles;
        private final String baseNameForPartitionedParquetData;
        private final ParquetFileLayout fileLayout;
        private final TableDefinition tableDefinition;
        private final Collection<List<String>> indexColumns;
        private final OnWriteCompleted onWriteCompleted;
        private final ParquetColumnResolver.Factory columnResolver;
        private final SeekableChannelsProvider seekableChannelsProviderForWriting;

        private ReadOnly(
                final KeyedObjectHashMap<String, ColumnInstructions> columnNameToInstructions,
                final KeyedObjectHashMap<String, ColumnInstructions> parquetColumnNameToColumnName,
                final String compressionCodecName,
                final int maximumDictionaryKeys,
                final int maximumDictionarySize,
                final boolean isLegacyParquet,
                final int targetPageSize,
                final boolean isRefreshing,
                final Object specialInstructions,
                final boolean generateMetadataFiles,
                final String baseNameForPartitionedParquetData,
                final ParquetFileLayout fileLayout,
                final TableDefinition tableDefinition,
                final Collection<List<String>> indexColumns,
                final OnWriteCompleted onWriteCompleted,
                final ParquetColumnResolver.Factory columnResolver,
                final SeekableChannelsProvider seekableChannelsProviderForWriting) {
            this.columnNameToInstructions = columnNameToInstructions;
            this.parquetColumnNameToInstructions = parquetColumnNameToColumnName;
            this.compressionCodecName = compressionCodecName;
            this.maximumDictionaryKeys = maximumDictionaryKeys;
            this.maximumDictionarySize = maximumDictionarySize;
            this.isLegacyParquet = isLegacyParquet;
            this.targetPageSize = targetPageSize;
            this.isRefreshing = isRefreshing;
            this.specialInstructions = specialInstructions;
            this.generateMetadataFiles = generateMetadataFiles;
            this.baseNameForPartitionedParquetData = baseNameForPartitionedParquetData;
            this.fileLayout = fileLayout;
            this.tableDefinition = tableDefinition;
            this.indexColumns = indexColumns == null ? null
                    : indexColumns.stream()
                            .map(List::copyOf)
                            .collect(Collectors.toUnmodifiableList());
            this.onWriteCompleted = onWriteCompleted;
            this.columnResolver = columnResolver;
            if (columnResolver != null) {
                if (tableDefinition == null) {
                    throw new IllegalArgumentException("When setting columnResolver, tableDefinition must be provided");
                }
            }
            this.seekableChannelsProviderForWriting = seekableChannelsProviderForWriting;
        }

        private <T> T getOrDefault(final String columnName, final T defaultValue,
                final Function<ColumnInstructions, T> fun) {
            if (columnNameToInstructions == null) {
                return defaultValue;
            }
            final ColumnInstructions ci = columnNameToInstructions.get(columnName);
            if (ci == null) {
                return defaultValue;
            }
            return fun.apply(ci);
        }

        private boolean getOrDefault(final String columnName, final boolean defaultValue,
                final Predicate<ColumnInstructions> fun) {
            if (columnNameToInstructions == null) {
                return defaultValue;
            }
            final ColumnInstructions ci = columnNameToInstructions.get(columnName);
            if (ci == null) {
                return defaultValue;
            }
            return fun.test(ci);
        }

        @Override
        public String getParquetColumnNameFromColumnNameOrDefault(final String columnName) {
            return getOrDefault(columnName, columnName, ColumnInstructions::getParquetColumnName);
        }

        @Override
        public String getColumnNameFromParquetColumnName(final String parquetColumnName) {
            if (parquetColumnNameToInstructions == null) {
                return null;
            }
            final ColumnInstructions ci = parquetColumnNameToInstructions.get(parquetColumnName);
            if (ci == null) {
                return null;
            }
            return ci.getColumnName();
        }

        @Override
        public String getCodecName(final String columnName) {
            return getOrDefault(columnName, null, ColumnInstructions::getCodecName);
        }

        @Override
        public String getCodecArgs(final String columnName) {
            return getOrDefault(columnName, null, ColumnInstructions::getCodecArgs);
        }

        @Override
        public boolean useDictionary(final String columnName) {
            return getOrDefault(columnName, false, ColumnInstructions::useDictionary);
        }

        @Override
        public OptionalInt getFieldId(String columnName) {
            return getOrDefault(columnName, OptionalInt.empty(), ColumnInstructions::fieldId);
        }

        @Override
        public String getCompressionCodecName() {
            return compressionCodecName;
        }

        @Override
        public int getMaximumDictionaryKeys() {
            return maximumDictionaryKeys;
        }

        @Override
        public int getMaximumDictionarySize() {
            return maximumDictionarySize;
        }

        @Override
        public boolean isLegacyParquet() {
            return isLegacyParquet;
        }

        @Override
        public int getTargetPageSize() {
            return targetPageSize;
        }

        @Override
        public boolean isRefreshing() {
            return isRefreshing;
        }

        @Override
        @Nullable
        public Object getSpecialInstructions() {
            return specialInstructions;
        }

        @Override
        public boolean generateMetadataFiles() {
            return generateMetadataFiles;
        }

        @Override
        public String baseNameForPartitionedParquetData() {
            return baseNameForPartitionedParquetData;
        }

        @Override
        public Optional<ParquetFileLayout> getFileLayout() {
            return Optional.ofNullable(fileLayout);
        }

        @Override
        public Optional<TableDefinition> getTableDefinition() {
            return Optional.ofNullable(tableDefinition);
        }

        @Override
        public Optional<Collection<List<String>>> getIndexColumns() {
            return Optional.ofNullable(indexColumns);
        }

        @Override
        public Optional<ParquetColumnResolver.Factory> getColumnResolverFactory() {
            return Optional.ofNullable(columnResolver);
        }

        @Override
        public Optional<SeekableChannelsProvider> getSeekableChannelsProviderForWriting() {
            return Optional.ofNullable(seekableChannelsProviderForWriting);
        }

        @Override
        public ParquetInstructions withTableDefinition(@Nullable final TableDefinition useDefinition) {
            return withTableDefinitionAndLayout(useDefinition, fileLayout);
        }

        @Override
        public ParquetInstructions withLayout(@Nullable final ParquetFileLayout useLayout) {
            return withTableDefinitionAndLayout(tableDefinition, useLayout);
        }

        @Override
        public ParquetInstructions withTableDefinitionAndLayout(
                @Nullable final TableDefinition useDefinition,
                @Nullable final ParquetFileLayout useLayout) {
            return new ReadOnly(columnNameToInstructions, parquetColumnNameToInstructions,
                    getCompressionCodecName(), getMaximumDictionaryKeys(), getMaximumDictionarySize(),
                    isLegacyParquet(), getTargetPageSize(), isRefreshing(), getSpecialInstructions(),
                    generateMetadataFiles(), baseNameForPartitionedParquetData(), useLayout, useDefinition,
                    indexColumns, onWriteCompleted, columnResolver, seekableChannelsProviderForWriting);
        }

        @Override
        ParquetInstructions withIndexColumns(final Collection<List<String>> useIndexColumns) {
            return new ReadOnly(columnNameToInstructions, parquetColumnNameToInstructions,
                    getCompressionCodecName(), getMaximumDictionaryKeys(), getMaximumDictionarySize(),
                    isLegacyParquet(), getTargetPageSize(), isRefreshing(), getSpecialInstructions(),
                    generateMetadataFiles(), baseNameForPartitionedParquetData(), fileLayout,
                    tableDefinition, useIndexColumns, onWriteCompleted, columnResolver,
                    seekableChannelsProviderForWriting);
        }

        @Override
        public Optional<OnWriteCompleted> onWriteCompleted() {
            return Optional.ofNullable(onWriteCompleted);
        }

        KeyedObjectHashMap<String, ColumnInstructions> copyColumnNameToInstructions() {
            // noinspection unchecked
            return (columnNameToInstructions == null)
                    ? null
                    : (KeyedObjectHashMap<String, ColumnInstructions>) columnNameToInstructions.clone();
        }

        KeyedObjectHashMap<String, ColumnInstructions> copyParquetColumnNameToInstructions() {
            // noinspection unchecked
            return (parquetColumnNameToInstructions == null)
                    ? null
                    : (KeyedObjectHashMap<String, ColumnInstructions>) parquetColumnNameToInstructions.clone();
        }

        private static boolean sameCodecMappings(final ReadOnly r1, final ReadOnly r2) {
            final Set<String> r1ColumnNames = r1.columnNameToInstructions.keySet();
            if (r2.columnNameToInstructions.size() != r1ColumnNames.size()) {
                return false;
            }
            for (String colName : r1ColumnNames) {
                if (!r2.columnNameToInstructions.containsKey(colName)) {
                    return false;
                }
                final String r1CodecName = r1.getCodecName(colName);
                final String r2CodecName = r2.getCodecName(colName);
                if (!Objects.equals(r1CodecName, r2CodecName)) {
                    return false;
                }
                final String r1CodecArgs = r1.getCodecArgs(colName);
                final String r2CodecArgs = r2.getCodecArgs(colName);
                if (!Objects.equals(r1CodecArgs, r2CodecArgs)) {
                    return false;
                }
            }
            return true;
        }
    }

    public static class Builder {
        private KeyedObjectHashMap<String, ColumnInstructions> columnNameToInstructions;
        // Note parquetColumnNameToInstructions may be null while columnNameToInstructions is not null;
        // We only store entries in parquetColumnNameToInstructions when the parquetColumnName is
        // different than the columnName (ie, the column name mapping is not the default mapping)
        private KeyedObjectHashMap<String, ColumnInstructions> parquetColumnNameToInstructions;
        private String compressionCodecName = DEFAULT_COMPRESSION_CODEC_NAME;
        private int maximumDictionaryKeys = DEFAULT_MAXIMUM_DICTIONARY_KEYS;
        private int maximumDictionarySize = DEFAULT_MAXIMUM_DICTIONARY_SIZE;
        private boolean isLegacyParquet;
        private int targetPageSize = DEFAULT_TARGET_PAGE_SIZE;
        private boolean isRefreshing = DEFAULT_IS_REFRESHING;
        private Object specialInstructions;
        private boolean generateMetadataFiles = DEFAULT_GENERATE_METADATA_FILES;
        private String baseNameForPartitionedParquetData = DEFAULT_BASE_NAME_FOR_PARTITIONED_PARQUET_DATA;
        private ParquetFileLayout fileLayout;
        private TableDefinition tableDefinition;
        private Collection<List<String>> indexColumns;
        private OnWriteCompleted onWriteCompleted;
        private ParquetColumnResolver.Factory columnResolverFactory;
        private SeekableChannelsProvider seekableChannelsProviderForWriting;

        /**
         * For each additional field added, make sure to update the copy constructor builder
         * {@link #Builder(ParquetInstructions)}
         */

        public Builder() {}

        public Builder(final ParquetInstructions parquetInstructions) {
            if (parquetInstructions == EMPTY) {
                return;
            }
            final ReadOnly readOnlyParquetInstructions = (ReadOnly) parquetInstructions;
            columnNameToInstructions = readOnlyParquetInstructions.copyColumnNameToInstructions();
            parquetColumnNameToInstructions = readOnlyParquetInstructions.copyParquetColumnNameToInstructions();
            compressionCodecName = readOnlyParquetInstructions.getCompressionCodecName();
            maximumDictionaryKeys = readOnlyParquetInstructions.getMaximumDictionaryKeys();
            maximumDictionarySize = readOnlyParquetInstructions.getMaximumDictionarySize();
            isLegacyParquet = readOnlyParquetInstructions.isLegacyParquet();
            targetPageSize = readOnlyParquetInstructions.getTargetPageSize();
            isRefreshing = readOnlyParquetInstructions.isRefreshing();
            specialInstructions = readOnlyParquetInstructions.getSpecialInstructions();
            generateMetadataFiles = readOnlyParquetInstructions.generateMetadataFiles();
            baseNameForPartitionedParquetData = readOnlyParquetInstructions.baseNameForPartitionedParquetData();
            fileLayout = readOnlyParquetInstructions.getFileLayout().orElse(null);
            tableDefinition = readOnlyParquetInstructions.getTableDefinition().orElse(null);
            indexColumns = readOnlyParquetInstructions.getIndexColumns().orElse(null);
            onWriteCompleted = readOnlyParquetInstructions.onWriteCompleted().orElse(null);
            columnResolverFactory = readOnlyParquetInstructions.getColumnResolverFactory().orElse(null);
            seekableChannelsProviderForWriting =
                    readOnlyParquetInstructions.getSeekableChannelsProviderForWriting().orElse(null);
        }

        public Builder addColumnNameMapping(final String parquetColumnName, final String columnName) {
            final ColumnInstructions ci = getOrCreateColumnInstructions(columnName);
            ci.setParquetColumnName(parquetColumnName);
            if (parquetColumnNameToInstructions == null) {
                parquetColumnNameToInstructions = new KeyedObjectHashMap<>(ColumnInstructions.PARQUET_COLUMN_NAME_KEY);
            }
            final ColumnInstructions existing = parquetColumnNameToInstructions.putIfAbsent(parquetColumnName, ci);
            if (existing != null) {
                // Note: this is a limitation that doesn't need to exist. Technically, we could allow a single physical
                // parquet column to manifest as multiple Deephaven columns.
                throw new IllegalArgumentException(
                        "Cannot add new mapping from parquetColumnName=" + parquetColumnName + " to columnName="
                                + columnName
                                + ": already mapped to columnName="
                                + existing.getColumnName());
            }
            return this;
        }

        public Set<String> getTakenNames() {
            return (columnNameToInstructions == null) ? Collections.emptySet() : columnNameToInstructions.keySet();
        }

        public Builder addColumnCodec(final String columnName, final String codecName) {
            return addColumnCodec(columnName, codecName, null);
        }

        public Builder addColumnCodec(final String columnName, final String codecName, final String codecArgs) {
            final ColumnInstructions ci = getOrCreateColumnInstructions(columnName);
            ci.setCodecName(codecName);
            ci.setCodecArgs(codecArgs);
            return this;
        }

        /**
         * Set a hint that the writer should use dictionary-based encoding for writing this column; never evaluated for
         * non-String columns.
         *
         * @param columnName The column name
         * @param useDictionary The hint value
         */
        public Builder useDictionary(final String columnName, final boolean useDictionary) {
            final ColumnInstructions ci = getOrCreateColumnInstructions(columnName);
            ci.useDictionary(useDictionary);
            return this;
        }

        /**
         * This is currently only used for writing, allowing the setting of {@code field_id} in the proper Parquet
         * {@code SchemaElement}.
         *
         * <p>
         * Setting multiple field ids for a single column name is not allowed.
         *
         * <p>
         * Field ids are not typically configured by end users.
         *
         * @param columnName the Deephaven column name
         * @param fieldId the field id
         */
        public Builder setFieldId(final String columnName, final int fieldId) {
            final ColumnInstructions ci = getOrCreateColumnInstructions(columnName);
            ci.setFieldId(fieldId);
            return this;
        }

        private ColumnInstructions getOrCreateColumnInstructions(final String columnName) {
            if (columnNameToInstructions == null) {
                columnNameToInstructions = new KeyedObjectHashMap<>(ColumnInstructions.COLUMN_NAME_KEY);
            }
            return columnNameToInstructions.putIfAbsent(columnName, ColumnInstructions::new);
        }

        public Builder setCompressionCodecName(final String compressionCodecName) {
            this.compressionCodecName = compressionCodecName;
            return this;
        }

        /**
         * Set the maximum number of unique keys the writer should add to a dictionary page before switching to
         * non-dictionary encoding; never evaluated for non-String columns, ignored if {@link #useDictionary(String) use
         * dictionary} is set for the column.
         *
         * @param maximumDictionaryKeys The maximum number of dictionary keys; must be {@code >= 0}
         */
        public Builder setMaximumDictionaryKeys(final int maximumDictionaryKeys) {
            this.maximumDictionaryKeys = Require.geqZero(maximumDictionaryKeys, "maximumDictionaryKeys");
            return this;
        }

        /**
         * Set the maximum number of bytes the writer should add to the dictionary before switching to non-dictionary
         * encoding; never evaluated for non-String columns, ignored if {@link #useDictionary(String) use dictionary} is
         * set for the column.
         *
         * @param maximumDictionarySize The maximum size of dictionary (in bytes); must be {@code >= 0}
         */
        public Builder setMaximumDictionarySize(final int maximumDictionarySize) {
            this.maximumDictionarySize = Require.geqZero(maximumDictionarySize, "maximumDictionarySize");
            return this;
        }

        public Builder setIsLegacyParquet(final boolean isLegacyParquet) {
            this.isLegacyParquet = isLegacyParquet;
            return this;
        }

        public Builder setTargetPageSize(final int targetPageSize) {
            if (targetPageSize < MIN_TARGET_PAGE_SIZE) {
                throw new IllegalArgumentException("Target page size should be >= " + MIN_TARGET_PAGE_SIZE);
            }
            this.targetPageSize = targetPageSize;
            return this;
        }

        public Builder setIsRefreshing(final boolean isRefreshing) {
            this.isRefreshing = isRefreshing;
            return this;
        }

        public Builder setSpecialInstructions(final Object specialInstructions) {
            this.specialInstructions = specialInstructions;
            return this;
        }

        /**
         * Set whether to generate {@value ParquetUtils#METADATA_FILE_NAME} and
         * {@value ParquetUtils#COMMON_METADATA_FILE_NAME} files while writing parquet files. On setting this parameter,
         * <ul>
         * <li>When writing a single parquet file, metadata files will be generated in the same parent directory as the
         * parquet file.
         * <li>When writing multiple parquet files in a single write call, the writing code insists that all parquet
         * files should be written to the same parent directory, and only then metadata files will be generated in the
         * same parent directory.
         * <li>When writing key-value partitioned parquet data, metadata files are generated in the root directory of
         * the partitioned parquet files.
         * </ul>
         */
        public Builder setGenerateMetadataFiles(final boolean generateMetadataFiles) {
            this.generateMetadataFiles = generateMetadataFiles;
            return this;
        }

        /**
         * Set the base name for partitioned parquet data. This is used to generate the file name for partitioned
         * parquet files, and therefore, this parameter is only used when writing partitioned parquet data. Users can
         * provide the following tokens to be replaced in the base name:
         * <ul>
         * <li>The token {@value #FILE_INDEX_TOKEN} will be replaced with an automatically incremented integer for files
         * in a directory. For example, a base name of "table-{i}" will result in files named like
         * "PC=partition1/table-0.parquet", "PC=partition1/table-1.parquet", etc., where PC is a partitioning
         * column.</li>
         * <li>The token {@value #UUID_TOKEN} will be replaced with a random UUID. For example, a base name of
         * "table-{uuid}" will result in files named like "table-8e8ab6b2-62f2-40d1-8191-1c5b70c5f330.parquet".</li>
         * <li>The token {@value #PARTITIONS_TOKEN} will be replaced with an underscore-delimited, concatenated string
         * of partition values. For example, a base name of "{partitions}-table" will result in files like
         * "PC1=partition1/PC2=partitionA/PC1=partition1_PC2=partitionA-table.parquet", where "PC1" and "PC2" are
         * partitioning columns.</li>
         * </ul>
         * The default value of this parameter is {@value #DEFAULT_BASE_NAME_FOR_PARTITIONED_PARQUET_DATA}.
         */
        public Builder setBaseNameForPartitionedParquetData(final String baseNameForPartitionedParquetData) {
            this.baseNameForPartitionedParquetData = baseNameForPartitionedParquetData;
            return this;
        }

        /**
         * Set the expected file layout when reading a parquet file or a directory. This info can be used to skip some
         * computations to deduce the file layout from the source directory structure.
         */
        public Builder setFileLayout(final ParquetFileLayout fileLayout) {
            this.fileLayout = fileLayout;
            return this;
        }

        /**
         * <ul>
         * <li>When reading a parquet file, this corresponds to the table definition to use instead of the one implied
         * by the parquet file being read. Providing a definition can help save additional computations to deduce the
         * table definition from the parquet files as well as from the directory layouts when reading partitioned
         * data.</li>
         * <li>When writing a parquet file, this corresponds to the table definition to use instead of the one implied
         * by the table being written</li>
         * </ul>
         * This definition can be used to skip some columns or add additional columns with {@code null} values.
         */
        public Builder setTableDefinition(final TableDefinition tableDefinition) {
            this.tableDefinition = tableDefinition;
            return this;
        }

        private void initIndexColumns() {
            if (indexColumns == null) {
                indexColumns = new ArrayList<>();
            }
        }

        /**
         * Add a list of columns to persist together as indexes. The write operation will store the index info as
         * sidecar tables. This argument is used to narrow the set of indexes to write, or to be explicit about the
         * expected set of indexes present on all sources. Indexes that are specified but missing will be computed on
         * demand.
         * <p>
         * Adding an index on an individual partitioning column is not allowed.
         */
        public Builder addIndexColumns(final String... indexColumns) {
            initIndexColumns();
            this.indexColumns.add(List.of(indexColumns));
            return this;
        }

        /**
         * Adds provided lists of columns to persist together as indexes. This method accepts an {@link Iterable} of
         * lists, where each list represents a group of columns to be indexed together.
         * <p>
         * The write operation will store the index info as sidecar tables. This argument is used to narrow the set of
         * indexes to write, or to be explicit about the expected set of indexes present on all sources. Indexes that
         * are specified but missing will be computed on demand. To prevent the generation of index files, provide an
         * empty iterable.
         * <p>
         * Adding an index on an individual partitioning column is not allowed.
         */
        public Builder addAllIndexColumns(final Iterable<List<String>> indexColumns) {
            initIndexColumns();
            for (final List<String> indexColumnList : indexColumns) {
                this.indexColumns.add(List.copyOf(indexColumnList));
            }
            return this;
        }

        /**
         * Adds a callback to be executed when on completing each parquet data file write (excluding the index and
         * metadata files).
         */
        public Builder setOnWriteCompleted(final OnWriteCompleted onWriteCompleted) {
            this.onWriteCompleted = onWriteCompleted;
            return this;
        }

        /**
         * Sets the column resolver factory to allow higher-level managers (such as Iceberg) to use advanced column
         * resolution logic based on the table key and table location key. When set,
         * {@link #setTableDefinition(TableDefinition)} must also be set. As such, the factory is <i>not</i> used for
         * inference purposes.
         *
         * <p>
         * This is not typically set by end-users.
         *
         * @param columnResolverFactory the column resolver factory
         */
        public Builder setColumnResolverFactory(ParquetColumnResolver.Factory columnResolverFactory) {
            this.columnResolverFactory = columnResolverFactory;
            return this;
        }

        /**
         * Sets the {@link SeekableChannelsProvider} to use for writing parquet files. This is used internally by the
         * Iceberg layer to provide a {@link SeekableChannelsProvider} for writing parquet data files.
         *
         * @param seekableChannelsProviderForWriting the {@link SeekableChannelsProvider} to use
         */
        @InternalUseOnly
        public Builder setSeekableChannelsProviderForWriting(
                SeekableChannelsProvider seekableChannelsProviderForWriting) {
            this.seekableChannelsProviderForWriting = seekableChannelsProviderForWriting;
            return this;
        }

        public ParquetInstructions build() {
            final KeyedObjectHashMap<String, ColumnInstructions> columnNameToInstructionsOut = columnNameToInstructions;
            columnNameToInstructions = null;
            final KeyedObjectHashMap<String, ColumnInstructions> parquetColumnNameToColumnNameOut =
                    parquetColumnNameToInstructions;
            parquetColumnNameToInstructions = null;
            return new ReadOnly(columnNameToInstructionsOut, parquetColumnNameToColumnNameOut, compressionCodecName,
                    maximumDictionaryKeys, maximumDictionarySize, isLegacyParquet, targetPageSize, isRefreshing,
                    specialInstructions, generateMetadataFiles, baseNameForPartitionedParquetData, fileLayout,
                    tableDefinition, indexColumns, onWriteCompleted, columnResolverFactory,
                    seekableChannelsProviderForWriting);
        }
    }

    public static Builder builder() {
        return new Builder();
    }
}
