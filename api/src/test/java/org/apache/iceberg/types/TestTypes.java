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
package org.apache.iceberg.types;

import org.apache.iceberg.types.Types.NestedField;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestTypes {

  @Test
  public void fromPrimitiveString() {
    Assertions.assertThat(Types.fromPrimitiveString("boolean")).isSameAs(Types.BooleanType.get());
    Assertions.assertThat(Types.fromPrimitiveString("BooLean")).isSameAs(Types.BooleanType.get());

    Assertions.assertThat(Types.fromPrimitiveString("timestamp"))
        .isSameAs(Types.TimestampType.withoutZone());

    Assertions.assertThat(Types.fromPrimitiveString("Fixed[ 3 ]"))
        .isEqualTo(Types.FixedType.ofLength(3));

    Assertions.assertThat(Types.fromPrimitiveString("Decimal( 2 , 3 )"))
        .isEqualTo(Types.DecimalType.of(2, 3));

    Assertions.assertThat(Types.fromPrimitiveString("Decimal(2,3)"))
        .isEqualTo(Types.DecimalType.of(2, 3));

    Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> Types.fromPrimitiveString("Unknown"))
        .withMessageContaining("Unknown");
  }

  @Test
  public void fieldDefaultsToNull() {
    NestedField field = NestedField.optional(1, "a", Types.IntegerType.get());
    Assertions.assertThat(field.initialDefault()).isNull();
    Assertions.assertThat(field.writeDefault()).isNull();
  }

  @Test
  public void fieldBuilderDefaultsToNull() {
    NestedField field = NestedField.required("a").withId(1).ofType(Types.IntegerType.get()).build();
    Assertions.assertThat(field.fieldId()).isEqualTo(1);
    Assertions.assertThat(field.name()).isEqualTo("a");
    Assertions.assertThat(field.isRequired()).isTrue();
    Assertions.assertThat(field.initialDefault()).isNull();
    Assertions.assertThat(field.writeDefault()).isNull();
  }

  @Test
  public void fieldBuilderWithDefaults() {
    NestedField field =
        NestedField.optional("a")
            .withId(1)
            .ofType(Types.IntegerType.get())
            .withDoc("doc")
            .withInitialDefault(34)
            .withWriteDefault(42)
            .build();

    Assertions.assertThat(field.isOptional()).isTrue();
    Assertions.assertThat(field.doc()).isEqualTo("doc");
    Assertions.assertThat(field.initialDefault()).isEqualTo(34);
    Assertions.assertThat(field.writeDefault()).isEqualTo(42);
  }

  @Test
  public void defaultsAreCastToFieldType() {
    // a Long literal is normalized to the Integer field type via Expressions.lit().to(type)
    NestedField field =
        NestedField.optional("a")
            .withId(1)
            .ofType(Types.IntegerType.get())
            .withInitialDefault(34L)
            .withWriteDefault(42L)
            .build();

    Assertions.assertThat(field.initialDefault()).isInstanceOf(Integer.class).isEqualTo(34);
    Assertions.assertThat(field.writeDefault()).isInstanceOf(Integer.class).isEqualTo(42);
  }

  @Test
  public void builderRequiresId() {
    Assertions.assertThatExceptionOfType(NullPointerException.class)
        .isThrownBy(() -> NestedField.required("a").ofType(Types.IntegerType.get()).build())
        .withMessageContaining("Id cannot be null");
  }

  @Test
  public void nestedTypeCannotHaveNonNullDefault() {
    Type nestedType = Types.ListType.ofOptional(2, Types.StringType.get());
    Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                NestedField.optional("a")
                    .withId(1)
                    .ofType(nestedType)
                    .withInitialDefault("x")
                    .build())
        .withMessageContaining("Invalid default value");
  }

  @Test
  public void fromCopiesDefaults() {
    NestedField field =
        NestedField.optional("a")
            .withId(1)
            .ofType(Types.IntegerType.get())
            .withInitialDefault(34)
            .withWriteDefault(42)
            .build();

    NestedField copy = NestedField.from(field).build();
    Assertions.assertThat(copy).isEqualTo(field);
    Assertions.assertThat(copy.initialDefault()).isEqualTo(34);
    Assertions.assertThat(copy.writeDefault()).isEqualTo(42);
  }

  @Test
  public void asOptionalAndAsRequiredPreserveDefaults() {
    NestedField field =
        NestedField.required("a")
            .withId(1)
            .ofType(Types.IntegerType.get())
            .withInitialDefault(34)
            .withWriteDefault(42)
            .build();

    NestedField optional = field.asOptional();
    Assertions.assertThat(optional.isOptional()).isTrue();
    Assertions.assertThat(optional.initialDefault()).isEqualTo(34);
    Assertions.assertThat(optional.writeDefault()).isEqualTo(42);

    NestedField required = optional.asRequired();
    Assertions.assertThat(required.isRequired()).isTrue();
    Assertions.assertThat(required).isEqualTo(field);
  }

  @Test
  public void equalsDistinguishesDefaults() {
    NestedField noDefault = NestedField.optional(1, "a", Types.IntegerType.get());
    NestedField withInitial = NestedField.from(noDefault).withInitialDefault(34).build();
    NestedField withWrite = NestedField.from(noDefault).withWriteDefault(42).build();

    Assertions.assertThat(withInitial).isNotEqualTo(noDefault);
    Assertions.assertThat(withWrite).isNotEqualTo(noDefault);
    Assertions.assertThat(withInitial).isNotEqualTo(withWrite);
  }
}
