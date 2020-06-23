package org.apache.iceberg.hive;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.HiveDecimalUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;


final class IcebergSchemaToTypeInfo {

    private IcebergSchemaToTypeInfo() {
    }

    private static final ImmutableMap<Object, Object> primitiveTypeToTypeInfo =
            ImmutableMap.builder()
                    .put(Types.BooleanType.get(),
                            TypeInfoFactory
                                    .getPrimitiveTypeInfo(
                                            serdeConstants.BOOLEAN_TYPE_NAME))
                    .put(Types.IntegerType.get(),
                            TypeInfoFactory
                                    .getPrimitiveTypeInfo(
                                            serdeConstants.INT_TYPE_NAME))
                    .put(Types.LongType.get(),
                            TypeInfoFactory
                                    .getPrimitiveTypeInfo(
                                            serdeConstants.BIGINT_TYPE_NAME))
                    .put(Types.FloatType.get(),
                            TypeInfoFactory
                                    .getPrimitiveTypeInfo(
                                            serdeConstants.FLOAT_TYPE_NAME))
                    .put(Types.DoubleType.get(),
                            TypeInfoFactory
                                    .getPrimitiveTypeInfo(
                                            serdeConstants.DOUBLE_TYPE_NAME))
                    .put(Types.BinaryType.get(),
                            TypeInfoFactory
                                    .getPrimitiveTypeInfo(
                                            serdeConstants.BINARY_TYPE_NAME))
                    .put(Types.StringType.get(),
                            TypeInfoFactory
                                    .getPrimitiveTypeInfo(
                                            serdeConstants.STRING_TYPE_NAME))
                    .put(Types.DateType.get(),
                            TypeInfoFactory
                                    .getPrimitiveTypeInfo(
                                            serdeConstants.DATE_TYPE_NAME))
                    .put(Types.TimestampType
                                    .withoutZone(),
                            TypeInfoFactory
                                    .getPrimitiveTypeInfo(
                                            serdeConstants.TIMESTAMP_TYPE_NAME))
                    .put(Types.TimestampType
                                    .withZone(),
                            TypeInfoFactory
                                    .getPrimitiveTypeInfo(
                                            serdeConstants.TIMESTAMP_TYPE_NAME))
                    .build();

    public static List<TypeInfo> getColumnTypes(Schema schema) {
        List<Types.NestedField> fields = schema.columns();
        List<TypeInfo> types = new ArrayList<>(fields.size());
        for (Types.NestedField field : fields) {
            types.add(generateTypeInfo(field.type()));
        }
        return types;
    }

    private static TypeInfo generateTypeInfo(Type type) {
        if (primitiveTypeToTypeInfo.containsKey(type)) {
            return (TypeInfo) primitiveTypeToTypeInfo.get(type);
        }
        switch (type.typeId()) {
            case TIME:
            case UUID:
                return TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME);
            case FIXED:
                return TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.BINARY_TYPE_NAME);
            case DECIMAL:
                Types.DecimalType dec = (Types.DecimalType) type;
                HiveDecimalUtils.validateParameter(dec.precision(), dec.scale());
                return TypeInfoFactory.getDecimalTypeInfo(dec.precision(), dec.scale());
            case STRUCT:
                return generateStructTypeInfo((Types.StructType) type);
            case LIST:
                return generateListTypeInfo((Types.ListType) type);
            case MAP:
                return generateMapTypeInfo((Types.MapType) type);
            default:
                throw new RuntimeException("Can't map Iceberg type to Hive TypeInfo: '" + type.typeId() + "'");
        }
    }

    private static TypeInfo generateMapTypeInfo(Types.MapType type) {
        Type keyType = type.keyType();
        Type valueType = type.valueType();
        return TypeInfoFactory.getMapTypeInfo(generateTypeInfo(keyType), generateTypeInfo(valueType));
    }

    private static TypeInfo generateStructTypeInfo(Types.StructType type) {
        List<Types.NestedField> fields = type.fields();
        List<String> fieldNames = new ArrayList<>(fields.size());
        List<TypeInfo> typeInfos = new ArrayList<>(fields.size());

        for (Types.NestedField field : fields) {
            fieldNames.add(field.name());
            typeInfos.add(generateTypeInfo(field.type()));
        }
        return TypeInfoFactory.getStructTypeInfo(fieldNames, typeInfos);
    }

    private static TypeInfo generateListTypeInfo(Types.ListType type) {
        return TypeInfoFactory.getListTypeInfo(generateTypeInfo(type.elementType()));
    }
}