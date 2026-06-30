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
import org.junit.Test;

/** Test projections on ORC types. */
public class TestBuildOrcProjection {

  @Test
  public void testProjectionPrimitiveNoOp() {
    Schema originalSchema =
        new Schema(
            optional(1, "a", Types.IntegerType.get()), optional(2, "b", Types.StringType.get()));

    // Original mapping (stored in ORC)
    TypeDescription orcSchema = ORCSchemaUtil.convert(originalSchema);
    assertEquals(2, orcSchema.getChildren().size());
    assertEquals(1, orcSchema.findSubtype("a").getId());
    assertEquals(TypeDescription.Category.INT, orcSchema.findSubtype("a").getCategory());
    assertEquals(2, orcSchema.findSubtype("b").getId());
    assertEquals(TypeDescription.Category.STRING, orcSchema.findSubtype("b").getCategory());
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
    assertEquals(2, newOrcSchema.getChildren().size());
    assertEquals(1, newOrcSchema.findSubtype("b").getId());
    assertEquals(TypeDescription.Category.STRING, newOrcSchema.findSubtype("b").getCategory());
    assertEquals(2, newOrcSchema.findSubtype("c_r3").getId());
    assertEquals(TypeDescription.Category.DATE, newOrcSchema.findSubtype("c_r3").getCategory());
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
    assertEquals(1, newOrcSchema.getChildren().size());
    assertEquals(TypeDescription.Category.STRUCT, newOrcSchema.findSubtype("a").getCategory());
    TypeDescription nestedCol = newOrcSchema.findSubtype("a");
    assertEquals(2, nestedCol.findSubtype("b").getId());
    assertEquals(TypeDescription.Category.STRING, nestedCol.findSubtype("b").getCategory());
    assertEquals(3, nestedCol.findSubtype("c").getId());
    assertEquals(TypeDescription.Category.DATE, nestedCol.findSubtype("c").getCategory());
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
    assertEquals(1, newOrcSchema.getChildren().size());
    assertEquals(TypeDescription.Category.STRUCT, newOrcSchema.findSubtype("a").getCategory());
    TypeDescription nestedCol = newOrcSchema.findSubtype("a");
    assertEquals(2, nestedCol.findSubtype("c").getId());
    assertEquals(TypeDescription.Category.DATE, nestedCol.findSubtype("c").getCategory());
    assertEquals(3, nestedCol.findSubtype("b").getId());
    assertEquals(TypeDescription.Category.STRING, nestedCol.findSubtype("b").getCategory());
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
    assertEquals(2, newOrcSchema.getChildren().size());
    assertEquals(TypeDescription.Category.INT, newOrcSchema.findSubtype("a").getCategory());
    assertEquals(2, newOrcSchema.findSubtype("b_r2").getId());
    assertEquals(TypeDescription.Category.STRUCT, newOrcSchema.findSubtype("b_r2").getCategory());
    TypeDescription nestedCol = newOrcSchema.findSubtype("b_r2");
    assertEquals(3, nestedCol.findSubtype("c_r3").getId());
    assertEquals(TypeDescription.Category.LONG, nestedCol.findSubtype("c_r3").getCategory());
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
  public void testTopLevelScalarDefaultOmittedWhenApplyDefaults() {
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

    // applyDefaults defaults to true: the absent top-level scalar default is omitted from the read
    // projection so the reader fills it as a constant via idToConstant.
    TypeDescription projection = ORCSchemaUtil.buildOrcProjection(evolvedSchema, baseOrcSchema);
    assertEquals(1, projection.getChildren().size());
    assertNotNull(projection.findSubtype("id"));
    assertFalse(
        "defaulted column must be omitted from the read projection",
        projection.getFieldNames().contains("country_r2"));
  }

  @Test
  public void testTopLevelScalarDefaultSynthesizedWhenApplyDefaultsFalse() {
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

    // applyDefaults=false (id-less / name-mapped read): the column is synthesized as a null column
    // instead of being omitted, so the default is never applied and the column reads NULL.
    TypeDescription projection =
        ORCSchemaUtil.buildOrcProjection(evolvedSchema, baseOrcSchema, false);
    assertEquals(2, projection.getChildren().size());
    assertEquals(2, projection.findSubtype("country_r2").getId());
    assertEquals(
        TypeDescription.Category.STRING, projection.findSubtype("country_r2").getCategory());
  }

  @Test
  public void testTopLevelRequiredScalarDefaultOmitted() {
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

    TypeDescription projection = ORCSchemaUtil.buildOrcProjection(evolvedSchema, baseOrcSchema);
    assertEquals(1, projection.getChildren().size());
    assertFalse(
        "required defaulted column must be omitted, not throw",
        projection.getFieldNames().contains("code_r2"));
  }

  @Test
  public void testNestedScalarDefaultOmitted() {
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

    TypeDescription projection = ORCSchemaUtil.buildOrcProjection(evolvedSchema, baseOrcSchema);
    TypeDescription nested = projection.findSubtype("s");
    assertEquals(1, nested.getChildren().size());
    assertFalse("nested defaulted column must be omitted", nested.getFieldNames().contains("b_r4"));
  }

  @Test
  public void testNestedStructEmptyAfterOmitKeepsPlaceholder() {
    // Base file: s { a }. Evolve to project only a new defaulted subfield s { b default 'x' } and
    // drop a. Every projected subfield of s is absent + defaulted, which would empty the nested
    // struct; the safeguard keeps one synthesized null column so the non-root struct is non-empty.
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

    TypeDescription projection = ORCSchemaUtil.buildOrcProjection(evolvedSchema, baseOrcSchema);
    TypeDescription nested = projection.findSubtype("s");
    assertEquals("non-root struct must keep at least one field", 1, nested.getChildren().size());
  }
}
