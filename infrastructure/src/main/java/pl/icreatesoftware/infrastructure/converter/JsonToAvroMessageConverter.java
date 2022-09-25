package pl.icreatesoftware.infrastructure.converter;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

@Component
@Slf4j
public class JsonToAvroMessageConverter {

    private static final List<Schema.Type> SCHEMA_PRIMITIVE_TYPES = List.of(
            Schema.Type.BOOLEAN,
            Schema.Type.INT,
            Schema.Type.LONG,
            Schema.Type.FLOAT,
            Schema.Type.DOUBLE,
            Schema.Type.BYTES,
            Schema.Type.STRING
    );

    private static final List<Schema.Type> SCHEMA_COMPLEX_TYPES = List.of(
            Schema.Type.RECORD,
            Schema.Type.ARRAY,
            Schema.Type.MAP,
            Schema.Type.UNION,
            Schema.Type.FIXED
    );

    public void prepareMessageBasedOnJson(GenericRecord record, JsonObject json) {
        Set<String> jsonFieldNames = json.keySet();

        jsonFieldNames.forEach(jsonFieldName -> {
            Optional<Schema.Field> fieldSchemaOptional = record.getSchema().getFields().stream()
                    .filter(schemaField -> Objects.equals(schemaField.name(), jsonFieldName))
                    .findFirst();

            if (fieldSchemaOptional.isEmpty()) {
                log.debug("Cannot find schema field by name: {}", jsonFieldName);
                return;
            }

            Schema.Field schemaFieldToFillData = fieldSchemaOptional.get();
            JsonElement jsonElement = json.get(jsonFieldName);

            copyValuesFromJsonToMessage(jsonElement, jsonFieldName, schemaFieldToFillData, record);
        });
    }

    private void copyValuesFromJsonToMessage(JsonElement jsonElement, String jsonFieldName, Schema.Field schemaFieldToFillData, GenericRecord record) {
        if (isPrimitiveType(jsonElement, schemaFieldToFillData)) {
            addPrimitive(record, jsonFieldName, jsonElement, schemaFieldToFillData);
        } else if (isComplexType(jsonElement, schemaFieldToFillData)) {
            addComplex(record, jsonFieldName, jsonElement, schemaFieldToFillData);
        } else {
            log.error("Cannot match types between provided Json and Schema.");
        }
    }

    private void addComplex(GenericRecord record, String jsonFieldName, JsonElement jsonElement, Schema.Field schemaFieldToFillData) {
        if (jsonElement.isJsonObject()) {
            log.debug("complex type - object");

            Optional<Schema> optionalSchema = schemaFieldToFillData.schema().getTypes().stream()
                    .filter(schema -> schema.getType().equals(Schema.Type.RECORD)).findFirst();

            if (optionalSchema.isEmpty()) {
                log.error("Cannot find correct sub schema for: {}", jsonFieldName);
                return;
            }

            GenericData.Record subRecord = new GenericData.Record(optionalSchema.get());
            prepareMessageBasedOnJson(subRecord, jsonElement.getAsJsonObject());
            record.put(jsonFieldName, subRecord);
        } else if (jsonElement.isJsonArray()) {
            log.debug("complex type - array");

            Optional<Schema> optionalSchema = schemaFieldToFillData.schema().getTypes().stream()
                    .filter(schema -> schema.getType().equals(Schema.Type.ARRAY)).findFirst();

            if (optionalSchema.isEmpty()) {
                log.error("Cannot find correct sub schema for: {}", jsonFieldName);
                return;
            }

            Schema schema = optionalSchema.get();
            var recordArray = prepareMessageBasedOnJson(schema, jsonElement.getAsJsonArray());
            record.put(jsonFieldName, recordArray);
        }
    }

    private GenericData.Array prepareMessageBasedOnJson(Schema schema, JsonArray jsonArray) {
        var recordArray = new GenericData.Array<>(jsonArray.size(), schema);

        jsonArray.forEach(jsonElement -> {
            if (jsonElement.isJsonPrimitive()) {
                if (jsonElement.getAsJsonPrimitive().isNumber()) {
                    recordArray.add(jsonElement.getAsNumber());
                }
                if (jsonElement.getAsJsonPrimitive().isBoolean()) {
                    recordArray.add(jsonElement.getAsBoolean());
                }
                if (jsonElement.getAsJsonPrimitive().isString()) {
                    recordArray.add(jsonElement.getAsString());
                }
            } else if (jsonElement.isJsonObject()) {
                var jsonObject = jsonElement.getAsJsonObject();
                if (schema.getElementType().getType() == Schema.Type.RECORD) {
                    jsonObject.keySet().forEach(objectName -> {
                        GenericData.Record record = new GenericData.Record(schema.getElementType());
                        record.put(objectName, jsonObject.get(objectName));
                    });
                }
            }
        });

        return recordArray;
    }

    private void addPrimitive(GenericRecord record, String jsonFieldName, JsonElement jsonElement, Schema.Field schemaFieldToFillData) {
        if (isAString(jsonElement, schemaFieldToFillData)) {
            record.put(jsonFieldName, jsonElement.getAsString());
            return;
        }
        if (isADate(jsonElement, schemaFieldToFillData)) {
            var instanceDate = ZonedDateTime.parse(jsonElement.getAsString()).toInstant();
            record.put(jsonFieldName, instanceDate);
            return;
        }
        if (isABoolean(jsonElement, schemaFieldToFillData)) {
            record.put(jsonFieldName, jsonElement.getAsBoolean());
            return;
        }
        if (isANumber(jsonElement, schemaFieldToFillData)) {
            record.put(jsonFieldName, jsonElement.getAsNumber());
        }
    }

    private boolean isANumber(JsonElement jsonElement, Schema.Field schemaFieldToFillData) {
        return jsonElement.getAsJsonPrimitive().isNumber() &&
                isSelectedTypeInFieldSchema(schemaFieldToFillData,
                        Schema.Type.INT,
                        Schema.Type.LONG,
                        Schema.Type.FLOAT,
                        Schema.Type.DOUBLE);
    }

    private boolean isAString(JsonElement jsonElement, Schema.Field schemaFieldToFillData) {
        return jsonElement.getAsJsonPrimitive().isString() &&
                isSelectedTypeInFieldSchema(schemaFieldToFillData, Schema.Type.STRING);
    }

    private boolean isABoolean(JsonElement jsonElement, Schema.Field schemaFieldToFillData) {
        return jsonElement.getAsJsonPrimitive().isBoolean() &&
                isSelectedTypeInFieldSchema(schemaFieldToFillData, Schema.Type.BOOLEAN);
    }

    private boolean isADate(JsonElement jsonElement, Schema.Field schemaFieldToFillData) {
        return jsonElement.getAsJsonPrimitive().isString() &&
                isSelectedTypeInFieldSchema(schemaFieldToFillData, Schema.Type.LONG);
    }

    private boolean isSelectedTypeInFieldSchema(Schema.Field schemaFieldToFillData, Schema.Type ... types) {
        if (schemaFieldToFillData.schema().isUnion()) {
            return schemaFieldToFillData
                    .schema()
                    .getTypes().stream()
                    .filter(fieldSchema -> List.of(types).contains(fieldSchema.getType())).count() == 1;
        }
        return List.of(types).contains(schemaFieldToFillData.schema().getType());
    }

    private static boolean isDateTypeFieldSchema(Schema.Field schemaFieldToFillData) {
        return schemaFieldToFillData
                .schema()
                .getTypes().stream()
                .filter(fieldSchema ->
                        fieldSchema.getType().equals(Schema.Type.LONG) &&
                                fieldSchema.getLogicalType()
                                        .equals(LogicalTypes.timestampMillis())).count() == 1;
    }

    private boolean isComplexType(JsonElement jsonElement, Schema.Field schemaFieldToFillData) {
        var isComplexTypeInSchema = schemaFieldToFillData
                .schema()
                .getTypes().stream()
                .filter(fieldSchema -> SCHEMA_COMPLEX_TYPES.contains(fieldSchema.getType())).count() == 1;

        return jsonElement.isJsonObject() && isComplexTypeInSchema;
    }

    private boolean isPrimitiveType(JsonElement jsonElement, Schema.Field schemaFieldToFillData) {
        var isPrimitiveTypeInSchema = false;
        if (schemaFieldToFillData.schema().isUnion()) {
            isPrimitiveTypeInSchema = schemaFieldToFillData
                    .schema()
                    .getTypes().stream()
                    .filter(fieldSchema -> SCHEMA_PRIMITIVE_TYPES.contains(fieldSchema.getType())).count() == 1;
        } else {
            isPrimitiveTypeInSchema = SCHEMA_PRIMITIVE_TYPES.contains(schemaFieldToFillData.schema().getType());
        }

        return jsonElement.isJsonPrimitive() && isPrimitiveTypeInSchema;
    }

}
