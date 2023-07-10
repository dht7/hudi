package org.apache.hudi.gcp.bigquery;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalType;

public class AvroToBQSchemaConverter {

  private static final String CONNECT_NAME = "connect.name";

  private static StandardSQLTypeName convertType(org.apache.avro.Schema schema) {
    if (schema.getLogicalType() != null) {
      return convertLogicalType(schema.getLogicalType());
    } else {
      return convertPrimitiveType(schema.getType());
    }
  }

  private static StandardSQLTypeName convertPrimitiveType(org.apache.avro.Schema.Type type) {
    switch (type) {
      case RECORD:
        return StandardSQLTypeName.STRUCT;
      case STRING:
        return StandardSQLTypeName.STRING;
      case INT:
        return StandardSQLTypeName.INT64;
      case LONG:
        return StandardSQLTypeName.INT64;
      case BOOLEAN:
        return StandardSQLTypeName.BOOL;
      case DOUBLE:
        return StandardSQLTypeName.FLOAT64;
      case FLOAT:
        return StandardSQLTypeName.FLOAT64;
      case BYTES:
        return StandardSQLTypeName.BYTES;
      case ENUM:
        return StandardSQLTypeName.STRING;
      default:
        return StandardSQLTypeName.STRING;
    }
  }

  private static StandardSQLTypeName convertLogicalType(LogicalType logicalType) {
    switch (logicalType.getName()) {
      case "decimal":
        return StandardSQLTypeName.FLOAT64;
      case "uuid":
        return StandardSQLTypeName.STRING;
      case "date":
        return StandardSQLTypeName.DATE;
      case "time-millis":
        return StandardSQLTypeName.TIME;
      case "time-micros":
        return StandardSQLTypeName.TIME;
      case "timestamp-millis":
        return StandardSQLTypeName.TIMESTAMP;
      case "timestamp-micros":
        return StandardSQLTypeName.TIMESTAMP;
      default:
        return StandardSQLTypeName.STRING;
    }
  }

  private static StandardSQLTypeName convertCustomType(String prop) {
    switch (prop) {
      case "custom.debezium.TimestampString":
        return StandardSQLTypeName.TIMESTAMP;
      case "custom.debezium.EpochDay":
        return StandardSQLTypeName.DATE;
      default:
        return StandardSQLTypeName.STRING;
    }
  }

  private static Field convertField(String name, org.apache.avro.Schema schema, Field.Mode mode) {
    if (schema.getType().equals(org.apache.avro.Schema.Type.RECORD)) {
      FieldList subFields = FieldList.of(schema.getFields().stream().map(f -> convertField(f.name(), f.schema(), Field.Mode.REQUIRED)).collect(Collectors.toList()));
      return Field.newBuilder(schema.getName(), StandardSQLTypeName.STRUCT, subFields).setMode(mode).build();
    } else if (schema.getType().equals(org.apache.avro.Schema.Type.ARRAY)) {
      return convertField(name, schema.getElementType(), Field.Mode.REPEATED);
    } else if (schema.getType().equals(org.apache.avro.Schema.Type.UNION)) {
      if (schema.getTypes().size() == 2) {

        org.apache.avro.Schema tempSchema;
        if (schema.getTypes().get(0).getType().equals(org.apache.avro.Schema.Type.NULL)) {
          tempSchema = schema.getTypes().get(1);
        } else if (schema.getTypes().get(1).getType().equals(org.apache.avro.Schema.Type.NULL)) {
          tempSchema = schema.getTypes().get(0);
        } else {
          throw new AvroTypeException("One of the union fields should have type `null`");
        }

        if (!tempSchema.getType().equals(org.apache.avro.Schema.Type.NULL)) {
          return convertField(name, tempSchema, Field.Mode.NULLABLE);
        } else {
          throw new AvroTypeException("Both of the union fields cannot have type `null`");
        }
      } else {
        throw new AvroTypeException("A Union type can only consist of two types, one of them should be `null`");
      }
    } else {
      return Field.newBuilder(name, convertType(schema)).setMode(mode).build();
    }
  }

  public static Schema convert(org.apache.avro.Schema avroSchema, List<String> partitionFields) {

    if (partitionFields.isEmpty()) {
      return Schema.of(avroSchema.getFields().stream().map(f -> convertField(f.name(), f.schema(), Field.Mode.REQUIRED)).collect(Collectors.toList()));
    } else {
      return Schema.of(avroSchema.getFields().stream().filter(f -> !partitionFields.contains(f.name())).map(f -> convertField(f.name(), f.schema(), Field.Mode.REQUIRED)).collect(Collectors.toList()));
    }
  }

}
