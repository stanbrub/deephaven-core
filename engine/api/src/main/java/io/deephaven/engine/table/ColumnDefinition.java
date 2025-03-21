//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.BoxedType;
import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.GenericVectorType;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.NativeArrayType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.StringType;
import io.deephaven.qst.type.Type;
import io.deephaven.vector.ByteVector;
import io.deephaven.vector.CharVector;
import io.deephaven.vector.DoubleVector;
import io.deephaven.vector.FloatVector;
import io.deephaven.vector.IntVector;
import io.deephaven.vector.LongVector;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ShortVector;
import io.deephaven.vector.Vector;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Column definition for all Deephaven columns.
 */
public class ColumnDefinition<TYPE> implements LogOutputAppendable {

    public static final ColumnDefinition<?>[] ZERO_LENGTH_COLUMN_DEFINITION_ARRAY = new ColumnDefinition[0];

    public enum ColumnType {
        /**
         * A normal column, with no special considerations.
         */
        Normal,

        /**
         * A column that helps define underlying partitions in the storage of the data, which consequently may also be
         * used for very efficient filtering.
         */
        Partitioning
    }

    public static ColumnDefinition<Boolean> ofBoolean(@NotNull final String name) {
        return new ColumnDefinition<>(name, Boolean.class);
    }

    public static ColumnDefinition<Byte> ofByte(@NotNull final String name) {
        return new ColumnDefinition<>(name, byte.class);
    }

    public static ColumnDefinition<Character> ofChar(@NotNull final String name) {
        return new ColumnDefinition<>(name, char.class);
    }

    public static ColumnDefinition<Short> ofShort(@NotNull final String name) {
        return new ColumnDefinition<>(name, short.class);
    }

    public static ColumnDefinition<Integer> ofInt(@NotNull final String name) {
        return new ColumnDefinition<>(name, int.class);
    }

    public static ColumnDefinition<Long> ofLong(@NotNull final String name) {
        return new ColumnDefinition<>(name, long.class);
    }

    public static ColumnDefinition<Float> ofFloat(@NotNull final String name) {
        return new ColumnDefinition<>(name, float.class);
    }

    public static ColumnDefinition<Double> ofDouble(@NotNull final String name) {
        return new ColumnDefinition<>(name, double.class);
    }

    public static ColumnDefinition<String> ofString(@NotNull final String name) {
        return new ColumnDefinition<>(name, String.class);
    }

    public static ColumnDefinition<Instant> ofTime(@NotNull final String name) {
        return new ColumnDefinition<>(name, Instant.class);
    }

    public static ColumnDefinition<?> of(String name, Type<?> type) {
        return type.walk(new Adapter(name));
    }

    public static ColumnDefinition<?> of(String name, PrimitiveType<?> type) {
        final PrimitiveType.Visitor<ColumnDefinition<?>> adapter = new Adapter(name);
        return type.walk(adapter);
    }

    public static ColumnDefinition<?> of(String name, GenericType<?> type) {
        final GenericType.Visitor<ColumnDefinition<?>> adapter = new Adapter(name);
        return type.walk(adapter);
    }

    public static <T extends Vector<?>> ColumnDefinition<T> ofVector(
            @NotNull final String name,
            @NotNull final Class<T> vectorType) {
        return new ColumnDefinition<>(name, vectorType, baseComponentTypeForVector(vectorType), ColumnType.Normal);
    }

    public static <T> ColumnDefinition<T> fromGenericType(
            @NotNull final String name,
            @NotNull final Class<T> dataType) {
        return fromGenericType(name, dataType, null);
    }

    public static <T> ColumnDefinition<T> fromGenericType(
            @NotNull final String name,
            @NotNull final Class<T> dataType,
            @Nullable final Class<?> componentType) {
        return fromGenericType(name, dataType, componentType, ColumnType.Normal);
    }

    public static <T> ColumnDefinition<T> fromGenericType(
            @NotNull final String name,
            @NotNull final Class<T> dataType,
            @Nullable final Class<?> componentType,
            @NotNull final ColumnType columnType) {
        return new ColumnDefinition<>(
                name, dataType, checkAndMaybeInferComponentType(dataType, componentType), columnType);
    }

    /**
     * Base component type class for each {@link Vector} type.
     */
    private static Class<?> baseComponentTypeForVector(@NotNull final Class<? extends Vector<?>> vectorType) {
        if (CharVector.class.isAssignableFrom(vectorType)) {
            return char.class;
        }
        if (ByteVector.class.isAssignableFrom(vectorType)) {
            return byte.class;
        }
        if (ShortVector.class.isAssignableFrom(vectorType)) {
            return short.class;
        }
        if (IntVector.class.isAssignableFrom(vectorType)) {
            return int.class;
        }
        if (LongVector.class.isAssignableFrom(vectorType)) {
            return long.class;
        }
        if (FloatVector.class.isAssignableFrom(vectorType)) {
            return float.class;
        }
        if (DoubleVector.class.isAssignableFrom(vectorType)) {
            return double.class;
        }
        if (ObjectVector.class.isAssignableFrom(vectorType)) {
            return Object.class;
        }
        throw new IllegalArgumentException("Unrecognized Vector type " + vectorType);
    }

    private static void assertComponentTypeValid(
            @NotNull final Class<?> dataType, @Nullable final Class<?> componentType) {
        if (!Vector.class.isAssignableFrom(dataType) && !dataType.isArray()) {
            return;
        }
        if (componentType == null) {
            throw new IllegalArgumentException("Required component type not specified for data type " + dataType);
        }
        if (dataType.isArray()) {
            final Class<?> arrayComponentType = dataType.getComponentType();
            if (!arrayComponentType.isAssignableFrom(componentType)) {
                throw new IllegalArgumentException(
                        "Invalid component type " + componentType + " for array data type " + dataType);
            }
            return;
        }
        // noinspection unchecked
        final Class<?> baseComponentType = baseComponentTypeForVector((Class<? extends Vector<?>>) dataType);
        if (!baseComponentType.isAssignableFrom(componentType)) {
            throw new IllegalArgumentException(
                    "Invalid component type " + componentType + " for Vector data type " + dataType);
        }
    }

    private static Class<?> checkAndMaybeInferComponentType(
            @NotNull final Class<?> dataType, @Nullable final Class<?> inputComponentType) {
        if (dataType.isArray()) {
            final Class<?> arrayComponentType = dataType.getComponentType();
            if (inputComponentType == null) {
                return arrayComponentType;
            }
            if (!arrayComponentType.isAssignableFrom(inputComponentType)) {
                throw new IllegalArgumentException(
                        "Invalid component type " + inputComponentType + " for array data type " + dataType);
            }
            return inputComponentType;
        }
        if (Vector.class.isAssignableFrom(dataType)) {
            // noinspection unchecked
            final Class<?> vectorComponentType =
                    baseComponentTypeForVector((Class<? extends Vector<?>>) dataType);
            if (inputComponentType == null) {
                /*
                 * TODO (https://github.com/deephaven/deephaven-core/issues/817): Allow formula results returning Vector
                 * to know component type if (Vector.class.isAssignableFrom(dataType)) { throw new
                 * IllegalArgumentException("Missing required component type for Vector data type " + dataType); }
                 */
                return vectorComponentType;
            }
            if (!vectorComponentType.isAssignableFrom(inputComponentType)) {
                throw new IllegalArgumentException(
                        "Invalid component type " + inputComponentType + " for Vector data type " + dataType);
            }
            return inputComponentType;
        }
        return inputComponentType;
    }

    public static ColumnDefinition<?> from(ColumnHeader<?> header) {
        return header.componentType().walk(new Adapter(header.name()));
    }

    private static class Adapter implements Type.Visitor<ColumnDefinition<?>>,
            PrimitiveType.Visitor<ColumnDefinition<?>>, GenericType.Visitor<ColumnDefinition<?>> {

        private final String name;

        public Adapter(String name) {
            this.name = Objects.requireNonNull(name);
        }

        @Override
        public ColumnDefinition<?> visit(PrimitiveType<?> primitiveType) {
            return primitiveType.walk((PrimitiveType.Visitor<ColumnDefinition<?>>) this);
        }

        @Override
        public ColumnDefinition<?> visit(GenericType<?> genericType) {
            return genericType.walk((GenericType.Visitor<ColumnDefinition<?>>) this);
        }

        @Override
        public ColumnDefinition<?> visit(BooleanType booleanType) {
            return ofBoolean(name);
        }

        @Override
        public ColumnDefinition<?> visit(ByteType byteType) {
            return ofByte(name);
        }

        @Override
        public ColumnDefinition<?> visit(CharType charType) {
            return ofChar(name);
        }

        @Override
        public ColumnDefinition<?> visit(ShortType shortType) {
            return ofShort(name);
        }

        @Override
        public ColumnDefinition<?> visit(IntType intType) {
            return ofInt(name);
        }

        @Override
        public ColumnDefinition<?> visit(LongType longType) {
            return ofLong(name);
        }

        @Override
        public ColumnDefinition<?> visit(FloatType floatType) {
            return ofFloat(name);
        }

        @Override
        public ColumnDefinition<?> visit(DoubleType doubleType) {
            return ofDouble(name);
        }

        @Override
        public ColumnDefinition<?> visit(BoxedType<?> boxedType) {
            // treat the same as primitive type
            return visit(boxedType.primitiveType());
        }

        @Override
        public ColumnDefinition<?> visit(StringType stringType) {
            return ofString(name);
        }

        @Override
        public ColumnDefinition<?> visit(InstantType instantType) {
            return ofTime(name);
        }

        @Override
        public ColumnDefinition<?> visit(ArrayType<?, ?> arrayType) {
            return arrayType.walk(new ArrayType.Visitor<>() {
                @Override
                public ColumnDefinition<?> visit(NativeArrayType<?, ?> nativeArrayType) {
                    return fromGenericType(name, nativeArrayType.clazz(), nativeArrayType.componentType().clazz());
                }

                @Override
                public ColumnDefinition<?> visit(PrimitiveVectorType<?, ?> vectorPrimitiveType) {
                    // noinspection unchecked
                    return ofVector(name, (Class<? extends Vector<?>>) vectorPrimitiveType.clazz());
                }

                @Override
                public ColumnDefinition<?> visit(GenericVectorType<?, ?> genericVectorType) {
                    return fromGenericType(name, ObjectVector.class, genericVectorType.componentType().clazz());
                }
            });
        }

        @Override
        public ColumnDefinition<?> visit(CustomType<?> customType) {
            return fromGenericType(name, customType.clazz());
        }
    }

    @NotNull
    private final String name;
    @NotNull
    private final Class<TYPE> dataType;
    @Nullable
    private final Class<?> componentType;
    @NotNull
    private final ColumnType columnType;

    private ColumnDefinition(@NotNull final String name, @NotNull final Class<TYPE> dataType) {
        this(name, dataType, null, ColumnType.Normal);
    }

    private ColumnDefinition(
            @NotNull final String name,
            @NotNull final Class<TYPE> dataType,
            @Nullable final Class<?> componentType,
            @NotNull final ColumnType columnType) {
        this.name = Objects.requireNonNull(name, "Column names cannot be null");
        this.dataType = Objects.requireNonNull(dataType);
        this.componentType = componentType;
        this.columnType = Objects.requireNonNull(columnType);
    }

    @NotNull
    public String getName() {
        return name;
    }

    @NotNull
    public Class<TYPE> getDataType() {
        return dataType;
    }

    @Nullable
    public Class<?> getComponentType() {
        return componentType;
    }

    @NotNull
    public ColumnType getColumnType() {
        return columnType;
    }

    public ColumnDefinition<TYPE> withPartitioning() {
        return isPartitioning() ? this : new ColumnDefinition<>(name, dataType, componentType, ColumnType.Partitioning);
    }

    public ColumnDefinition<TYPE> withNormal() {
        return columnType == ColumnType.Normal
                ? this
                : new ColumnDefinition<>(name, dataType, componentType, ColumnType.Normal);
    }

    public <Other> ColumnDefinition<Other> withDataType(@NotNull final Class<Other> newDataType) {
        // noinspection unchecked
        return dataType == newDataType
                ? (ColumnDefinition<Other>) this
                : fromGenericType(name, newDataType, componentType, columnType);
    }

    public <Other> ColumnDefinition<Other> withDataType(
            @NotNull final Class<Other> newDataType,
            @Nullable final Class<?> newComponentType) {
        // noinspection unchecked
        return dataType == newDataType && componentType == newComponentType
                ? (ColumnDefinition<Other>) this
                : fromGenericType(name, newDataType, newComponentType, columnType);
    }

    public ColumnDefinition<?> withName(@NotNull final String newName) {
        return newName.equals(name) ? this : new ColumnDefinition<>(newName, dataType, componentType, columnType);
    }

    public boolean isPartitioning() {
        return (columnType == ColumnType.Partitioning);
    }

    public boolean isDirect() {
        return (columnType == ColumnType.Normal);
    }

    /**
     * Compares two ColumnDefinitions somewhat more permissively than equals, disregarding matters of storage and
     * derivation. Checks for equality of {@code name}, {@code dataType}, and {@code componentType}. As such, this
     * method has an equivalence relation, ie {@code A.isCompatible(B) == B.isCompatible(A)}.
     *
     * @param other The ColumnDefinition to compare to
     * @return Whether the ColumnDefinition defines a column whose name and data are compatible with this
     *         ColumnDefinition
     */
    public boolean isCompatible(@NotNull final ColumnDefinition<?> other) {
        if (this == other) {
            return true;
        }
        return this.name.equals(other.name)
                && this.dataType == other.dataType
                && this.componentType == other.componentType;
    }

    /**
     * Compares two ColumnDefinitions somewhat more permissively than equals, disregarding matters of name, storage and
     * derivation. Checks for equality of {@code dataType}, and {@code componentType}. As such, this method has an
     * equivalence relation, ie {@code A.hasCompatibleDataType(B) == B.hasCompatibleDataType(A)}.
     *
     * @param other - The ColumnDefinition to compare to.
     * @return True if the ColumnDefinition defines a column whose data is compatible with this ColumnDefinition.
     */
    public boolean hasCompatibleDataType(@NotNull final ColumnDefinition<?> other) {
        return dataType == other.dataType && componentType == other.componentType;
    }

    /**
     * Describes the column definition with respect to the fields that are checked in
     * {@link #isCompatible(ColumnDefinition)}.
     *
     * @return the description for compatibility
     */
    public String describeForCompatibility() {
        if (componentType == null) {
            return String.format("[%s, %s]", name, dataType);
        }
        return String.format("[%s, %s, %s]", name, dataType, componentType);
    }

    /**
     * Enumerate the differences between this ColumnDefinition, and another one. Lines will be of the form "lhs
     * attribute 'value' does not match rhs attribute 'value'.
     *
     * @param differences an array to which differences can be added
     * @param other the ColumnDefinition under comparison
     * @param lhs what to call "this" definition
     * @param rhs what to call the other definition
     * @param prefix begin each difference with this string
     * @param includeColumnType whether to include {@code columnType} comparisons
     */
    public void describeDifferences(@NotNull List<String> differences, @NotNull final ColumnDefinition<?> other,
            @NotNull final String lhs, @NotNull final String rhs, @NotNull final String prefix,
            final boolean includeColumnType) {
        if (this == other) {
            return;
        }
        if (!name.equals(other.name)) {
            differences.add(prefix + lhs + " name '" + name + "' does not match " + rhs + " name '" + other.name + "'");
        }
        if (dataType != other.dataType) {
            differences.add(prefix + lhs + " dataType '" + dataType + "' does not match " + rhs + " dataType '"
                    + other.dataType + "'");
        } else {
            if (componentType != other.componentType) {
                differences.add(prefix + lhs + " componentType '" + componentType + "' does not match " + rhs
                        + " componentType '" + other.componentType + "'");
            }
            if (includeColumnType && columnType != other.columnType) {
                differences.add(prefix + lhs + " columnType " + columnType + " does not match " + rhs + " columnType "
                        + other.columnType);
            }
        }
    }

    /**
     * Checks if objects of type {@link #getDataType() dataType} can be cast to {@code destDataType} (equivalent to
     * {@code destDataType.isAssignableFrom(dataType)}). If not, this throws a {@link ClassCastException}.
     *
     * @param destDataType the destination data type
     */
    public final void checkCastTo(Class<?> destDataType) {
        TypeHelper.checkCastTo("[" + name + "]", dataType, destDataType);
    }

    /**
     * Checks if objects of type {@link #getDataType() dataType} can be cast to {@code destDataType} (equivalent to
     * {@code destDataType.isAssignableFrom(dataType)}) and checks that objects of type {@link #getComponentType()
     * componentType} can be cast to {@code destComponentType} (both component types must be present and cast-able, or
     * both must be {@code null}; when both present, is equivalent to
     * {@code destComponentType.isAssignableFrom(componentType)}). If not, this throws a {@link ClassCastException}.
     *
     * @param destDataType the destination data type
     * @param destComponentType the destination component type, may be {@code null}
     */
    public final void checkCastTo(Class<?> destDataType, @Nullable Class<?> destComponentType) {
        TypeHelper.checkCastTo("[" + name + "]", dataType, componentType, destDataType, destComponentType);
    }

    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ColumnDefinition)) {
            return false;
        }
        final ColumnDefinition<?> otherCD = (ColumnDefinition<?>) other;
        return name.equals(otherCD.name)
                && dataType == otherCD.dataType
                && componentType == otherCD.componentType
                && columnType == otherCD.columnType;
    }

    @Override
    public int hashCode() {
        return (((31
                + name.hashCode()) * 31
                + dataType.hashCode()) * 31
                + Objects.hashCode(componentType)) * 31
                + columnType.hashCode();
    }

    @Override
    public String toString() {
        return new LogOutputStringImpl().append(this).toString();
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append("ColumnDefinition {")
                .append("name=").append(name)
                .append(", dataType=").append(String.valueOf(dataType))
                .append(", componentType=").append(String.valueOf(componentType))
                .append(", columnType=").append(columnType.name())
                .append('}');
    }
}
