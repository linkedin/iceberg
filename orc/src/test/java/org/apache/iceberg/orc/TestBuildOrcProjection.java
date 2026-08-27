/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.orc;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test projections on ORC types. */
public class TestBuildOrcProjection {

  @Test
  public void testProjectionPrimitiveNoOp() {
    Schema originalSchema =
        new Schema(
            optional(1, "a", Types.IntegerType.get()), optional(2, "b", Types.StringType.get()));

    // Original mapping (stored in ORC)
    TypeDescription orcSchema = ORCSchemaUtil.convert(originalSchema);
    Assertions.assertThat(orcSchema.getChildren()).hasSize(2);
    Assertions.assertThat(orcSchema.findSubtype("a").getId()).isEqualTo(1);
    Assertions.assertThat(orcSchema.findSubtype("a").getCategory())
        .isEqualTo(TypeDescription.Category.INT);
    Assertions.assertThat(orcSchema.findSubtype("b").getId()).isEqualTo(2);
    Assertions.assertThat(orcSchema.findSubtype("b").getCategory())
        .isEqualTo(TypeDescription.Category.STRING);
  }

  @Test
  public void testProjectionPrimitive() {
    Schema originalSchema =
        new Schema(
            optional(1, "a", Types.IntegerType.get()), optional(2, "b", Types.StringType.get()));

    // Original mapping (stored in ORC)
    TypeDescription orcSchema = ORCSchemaUtil.convert(originalSchema);

    // Evolve schema
    Schema evolveSchema =
        new Schema(
            optional(2, "a", Types.StringType.get()),
            optional(3, "c", Types.DateType.get()) // will produce ORC column c_r3 (new)
            );

    TypeDescription newOrcSchema = ORCSchemaUtil.buildOrcProjection(evolveSchema, orcSchema);
    Assertions.assertThat(newOrcSchema.getChildren()).hasSize(2);
    Assertions.assertThat(newOrcSchema.findSubtype("b").getId()).isEqualTo(1);
    Assertions.assertThat(newOrcSchema.findSubtype("b").getCategory())
        .isEqualTo(TypeDescription.Category.STRING);
    Assertions.assertThat(newOrcSchema.findSubtype("c_r3").getId()).isEqualTo(2);
    Assertions.assertThat(newOrcSchema.findSubtype("c_r3").getCategory())
        .isEqualTo(TypeDescription.Category.DATE);
  }

  @Test
  public void testProjectionNestedNoOp() {
    Types.StructType nestedStructType =
        Types.StructType.of(
            optional(2, "b", Types.StringType.get()), optional(3, "c", Types.DateType.get()));
    Schema originalSchema = new Schema(optional(1, "a", nestedStructType));

    // Original mapping (stored in ORC)
    TypeDescription orcSchema = ORCSchemaUtil.convert(originalSchema);

    TypeDescription newOrcSchema = ORCSchemaUtil.buildOrcProjection(originalSchema, orcSchema);
    Assertions.assertThat(newOrcSchema.getChildren()).hasSize(1);
    Assertions.assertThat(newOrcSchema.findSubtype("a").getCategory())
        .isEqualTo(TypeDescription.Category.STRUCT);
    TypeDescription nestedCol = newOrcSchema.findSubtype("a");
    Assertions.assertThat(nestedCol.findSubtype("b").getId()).isEqualTo(2);
    Assertions.assertThat(nestedCol.findSubtype("b").getCategory())
        .isEqualTo(TypeDescription.Category.STRING);
    Assertions.assertThat(nestedCol.findSubtype("c").getId()).isEqualTo(3);
    Assertions.assertThat(nestedCol.findSubtype("c").getCategory())
        .isEqualTo(TypeDescription.Category.DATE);
  }

  @Test
  public void testProjectionNested() {
    Types.StructType nestedStructType =
        Types.StructType.of(
            optional(2, "b", Types.StringType.get()), optional(3, "c", Types.DateType.get()));
    Schema originalSchema = new Schema(optional(1, "a", nestedStructType));

    // Original mapping (stored in ORC)
    TypeDescription orcSchema = ORCSchemaUtil.convert(originalSchema);

    // Evolve schema
    Types.StructType newNestedStructType =
        Types.StructType.of(
            optional(3, "cc", Types.DateType.get()), optional(2, "bb", Types.StringType.get()));
    Schema evolveSchema = new Schema(optional(1, "aa", newNestedStructType));

    TypeDescription newOrcSchema = ORCSchemaUtil.buildOrcProjection(evolveSchema, orcSchema);
    Assertions.assertThat(newOrcSchema.getChildren()).hasSize(1);
    Assertions.assertThat(newOrcSchema.findSubtype("a").getCategory())
        .isEqualTo(TypeDescription.Category.STRUCT);
    TypeDescription nestedCol = newOrcSchema.findSubtype("a");
    Assertions.assertThat(nestedCol.findSubtype("c").getId()).isEqualTo(2);
    Assertions.assertThat(nestedCol.findSubtype("c").getCategory())
        .isEqualTo(TypeDescription.Category.DATE);
    Assertions.assertThat(nestedCol.findSubtype("b").getId()).isEqualTo(3);
    Assertions.assertThat(nestedCol.findSubtype("b").getCategory())
        .isEqualTo(TypeDescription.Category.STRING);
  }

  @Test
  public void testEvolutionAddContainerField() {
    Schema baseSchema = new Schema(required(1, "a", Types.IntegerType.get()));
    TypeDescription baseOrcSchema = ORCSchemaUtil.convert(baseSchema);

    Schema evolvedSchema =
        new Schema(
            required(1, "a", Types.IntegerType.get()),
            optional(2, "b", Types.StructType.of(required(3, "c", Types.LongType.get()))));

    TypeDescription newOrcSchema = ORCSchemaUtil.buildOrcProjection(evolvedSchema, baseOrcSchema);
    Assertions.assertThat(newOrcSchema.getChildren()).hasSize(2);
    Assertions.assertThat(newOrcSchema.findSubtype("a").getCategory())
        .isEqualTo(TypeDescription.Category.INT);
    Assertions.assertThat(newOrcSchema.findSubtype("b_r2").getId()).isEqualTo(2);
    Assertions.assertThat(newOrcSchema.findSubtype("b_r2").getCategory())
        .isEqualTo(TypeDescription.Category.STRUCT);
    TypeDescription nestedCol = newOrcSchema.findSubtype("b_r2");
    Assertions.assertThat(nestedCol.findSubtype("c_r3").getId()).isEqualTo(3);
    Assertions.assertThat(nestedCol.findSubtype("c_r3").getCategory())
        .isEqualTo(TypeDescription.Category.LONG);
  }

  @Test
  public void testRequiredNestedFieldMissingInFile() {
    Schema baseSchema =
        new Schema(
            required(1, "a", Types.IntegerType.get()),
            required(2, "b", Types.StructType.of(required(3, "c", Types.LongType.get()))));
    TypeDescription baseOrcSchema = ORCSchemaUtil.convert(baseSchema);

    Schema evolvedSchema =
        new Schema(
            required(1, "a", Types.IntegerType.get()),
            required(
                2,
                "b",
                Types.StructType.of(
                    required(3, "c", Types.LongType.get()),
                    required(4, "d", Types.LongType.get()))));

    Assertions.assertThatThrownBy(
            () -> ORCSchemaUtil.buildOrcProjection(evolvedSchema, baseOrcSchema))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Field 4 of type long is required and was not found.");
  }

  @Test
  public void testOmitsTopLevelScalarDefaultWhenReaderSupportsDefaultsAndIdsAreEmbedded() {
    Schema baseSchema = new Schema(required(1, "id", Types.LongType.get()));
    TypeDescription baseOrcSchema = ORCSchemaUtil.convert(baseSchema);

    Schema evolvedSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            Types.NestedField.optional("country")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault(Expressions.lit("US"))
                .build());

    // The file carries embedded field IDs, so the absent field can be identified safely.
    TypeDescription projection =
        ORCSchemaUtil.buildOrcProjection(
            evolvedSchema, baseOrcSchema, ORCSchemaUtil.FieldIdSource.EMBEDDED, true);
    assertEquals(1, projection.getChildren().size());
    assertNotNull(projection.findSubtype("id"));
    assertFalse(
        "defaulted column must be omitted from the read projection",
        projection.getFieldNames().contains("country_r2"));
  }

  @Test
  public void testSynthesizesNullForTopLevelScalarDefaultWhenReaderDoesNotSupportDefaults() {
    Schema baseSchema = new Schema(required(1, "id", Types.LongType.get()));
    TypeDescription baseOrcSchema = ORCSchemaUtil.convert(baseSchema);
    Schema evolvedSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            Types.NestedField.optional("country")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault(Expressions.lit("US"))
                .build());

    TypeDescription projection =
        ORCSchemaUtil.buildOrcProjection(
            evolvedSchema, baseOrcSchema, ORCSchemaUtil.FieldIdSource.EMBEDDED, false);

    assertEquals(2, projection.getChildren().size());
    assertNotNull(projection.findSubtype("country_r2"));
  }

  @Test
  public void testSynthesizesNullForTopLevelScalarDefaultWhenIdsAreNameMapped() {
    Schema baseSchema = new Schema(required(1, "id", Types.LongType.get()));
    TypeDescription baseOrcSchema = ORCSchemaUtil.convert(baseSchema);

    Schema evolvedSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            Types.NestedField.optional("country")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault(Expressions.lit("US"))
                .build());

    // Name-mapped provenance (or the conservative public 2-arg API): an unmatched name does not
    // prove the column is absent, so synthesize NULL rather than applying the default.
    TypeDescription projection =
        ORCSchemaUtil.buildOrcProjection(
            evolvedSchema, baseOrcSchema, ORCSchemaUtil.FieldIdSource.NAME_MAPPED, true);
    assertEquals(2, projection.getChildren().size());
    assertEquals(2, projection.findSubtype("country_r2").getId());
    assertEquals(
        TypeDescription.Category.STRING, projection.findSubtype("country_r2").getCategory());
  }

  @Test
  public void testOmitsRequiredTopLevelScalarDefaultWhenReaderSupportsDefaults() {
    Schema baseSchema = new Schema(required(1, "id", Types.LongType.get()));
    TypeDescription baseOrcSchema = ORCSchemaUtil.convert(baseSchema);

    // A required top-level field that is absent from the file but declares a default must be
    // omitted (then filled), not rejected by the required-missing check.
    Schema evolvedSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            Types.NestedField.required("code")
                .withId(2)
                .ofType(Types.IntegerType.get())
                .withInitialDefault(Expressions.lit(7))
                .build());

    TypeDescription projection =
        ORCSchemaUtil.buildOrcProjection(
            evolvedSchema, baseOrcSchema, ORCSchemaUtil.FieldIdSource.EMBEDDED, true);
    assertEquals(1, projection.getChildren().size());
    assertFalse(
        "required defaulted column must be omitted, not throw",
        projection.getFieldNames().contains("code_r2"));
  }

  @Test
  public void testOmitsNestedScalarDefaultWhenReaderSupportsDefaults() {
    Schema baseSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "s", Types.StructType.of(required(3, "a", Types.LongType.get()))));
    TypeDescription baseOrcSchema = ORCSchemaUtil.convert(baseSchema);

    // A scalar default on a field nested inside a struct is omitted (then filled via idToConstant),
    // at any nesting level. The present sibling "a" keeps the struct non-empty.
    Schema evolvedSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "s",
                Types.StructType.of(
                    required(3, "a", Types.LongType.get()),
                    Types.NestedField.optional("b")
                        .withId(4)
                        .ofType(Types.StringType.get())
                        .withInitialDefault(Expressions.lit("x"))
                        .build())));

    TypeDescription projection =
        ORCSchemaUtil.buildOrcProjection(
            evolvedSchema, baseOrcSchema, ORCSchemaUtil.FieldIdSource.EMBEDDED, true);
    TypeDescription nested = projection.findSubtype("s");
    assertEquals(1, nested.getChildren().size());
    assertFalse("nested defaulted column must be omitted", nested.getFieldNames().contains("b_r4"));
  }

  @Test
  public void testPreservesNestedStructWhenAllProjectedFieldsAreOmitted() {
    // Base file: s { a }. Project only a new defaulted subfield s { b default 'x' } (drop a). Every
    // projected subfield of s is absent + defaulted, so the nested read struct is omitted down to
    // empty; the reader fills b via idToConstant.
    Schema baseSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "s", Types.StructType.of(required(3, "a", Types.LongType.get()))));
    TypeDescription baseOrcSchema = ORCSchemaUtil.convert(baseSchema);

    Schema evolvedSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                2,
                "s",
                Types.StructType.of(
                    Types.NestedField.optional("b")
                        .withId(4)
                        .ofType(Types.StringType.get())
                        .withInitialDefault(Expressions.lit("x"))
                        .build())));

    TypeDescription projection =
        ORCSchemaUtil.buildOrcProjection(
            evolvedSchema, baseOrcSchema, ORCSchemaUtil.FieldIdSource.EMBEDDED, true);
    TypeDescription nested = projection.findSubtype("s");
    assertEquals(0, nested.getChildren().size());
  }
}
