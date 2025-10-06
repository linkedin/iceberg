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
package org.apache.iceberg.actions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.RewriteJobOrder;
import org.junit.jupiter.api.Test;

public class TestRewritePositionDeletesGroup {

  @Test
  public void testComparatorBytesAsc() {
    RewritePositionDeletesGroup group1 = createMockGroup(100L, 1, 1L);
    RewritePositionDeletesGroup group2 = createMockGroup(200L, 2, 2L);
    RewritePositionDeletesGroup group3 = createMockGroup(150L, 3, 3L);

    List<RewritePositionDeletesGroup> groups = Arrays.asList(group2, group3, group1);
    groups.sort(RewritePositionDeletesGroup.comparator(RewriteJobOrder.BYTES_ASC));

    assertThat(
            groups.stream()
                .mapToLong(RewritePositionDeletesGroup::rewrittenBytes)
                .boxed()
                .collect(Collectors.toList()))
        .containsExactly(100L, 150L, 200L);
  }

  @Test
  public void testComparatorBytesDesc() {
    RewritePositionDeletesGroup group1 = createMockGroup(100L, 1, 1L);
    RewritePositionDeletesGroup group2 = createMockGroup(200L, 2, 2L);
    RewritePositionDeletesGroup group3 = createMockGroup(150L, 3, 3L);

    List<RewritePositionDeletesGroup> groups = Arrays.asList(group2, group3, group1);
    groups.sort(RewritePositionDeletesGroup.comparator(RewriteJobOrder.BYTES_DESC));

    assertThat(
            groups.stream()
                .mapToLong(RewritePositionDeletesGroup::rewrittenBytes)
                .boxed()
                .collect(Collectors.toList()))
        .containsExactly(200L, 150L, 100L);
  }

  @Test
  public void testComparatorFilesAsc() {
    RewritePositionDeletesGroup group1 = createMockGroup(100L, 1, 1L);
    RewritePositionDeletesGroup group2 = createMockGroup(200L, 3, 2L);
    RewritePositionDeletesGroup group3 = createMockGroup(150L, 2, 3L);

    List<RewritePositionDeletesGroup> groups = Arrays.asList(group2, group3, group1);
    groups.sort(RewritePositionDeletesGroup.comparator(RewriteJobOrder.FILES_ASC));

    assertThat(
            groups.stream()
                .mapToInt(RewritePositionDeletesGroup::numRewrittenDeleteFiles)
                .boxed()
                .collect(Collectors.toList()))
        .containsExactly(1, 2, 3);
  }

  @Test
  public void testComparatorFilesDesc() {
    RewritePositionDeletesGroup group1 = createMockGroup(100L, 1, 1L);
    RewritePositionDeletesGroup group2 = createMockGroup(200L, 3, 2L);
    RewritePositionDeletesGroup group3 = createMockGroup(150L, 2, 3L);

    List<RewritePositionDeletesGroup> groups = Arrays.asList(group2, group3, group1);
    groups.sort(RewritePositionDeletesGroup.comparator(RewriteJobOrder.FILES_DESC));

    assertThat(
            groups.stream()
                .mapToInt(RewritePositionDeletesGroup::numRewrittenDeleteFiles)
                .boxed()
                .collect(Collectors.toList()))
        .containsExactly(3, 2, 1);
  }

  @Test
  public void testComparatorFilesMinSequenceNumberAsc() {
    RewritePositionDeletesGroup group1 = createMockGroup(100L, 1, 5L);
    RewritePositionDeletesGroup group2 = createMockGroup(200L, 2, 2L);
    RewritePositionDeletesGroup group3 = createMockGroup(150L, 3, 8L);

    List<RewritePositionDeletesGroup> groups = Arrays.asList(group1, group3, group2);
    groups.sort(
        RewritePositionDeletesGroup.comparator(RewriteJobOrder.FILES_MIN_SEQUENCE_NUMBER_ASC));

    assertThat(
            groups.stream()
                .mapToLong(RewritePositionDeletesGroup::minDataSequenceNumber)
                .boxed()
                .collect(Collectors.toList()))
        .containsExactly(2L, 5L, 8L);
  }

  @Test
  public void testComparatorFilesMinSequenceNumberDesc() {
    RewritePositionDeletesGroup group1 = createMockGroup(100L, 1, 5L);
    RewritePositionDeletesGroup group2 = createMockGroup(200L, 2, 2L);
    RewritePositionDeletesGroup group3 = createMockGroup(150L, 3, 8L);

    List<RewritePositionDeletesGroup> groups = Arrays.asList(group1, group3, group2);
    groups.sort(
        RewritePositionDeletesGroup.comparator(RewriteJobOrder.FILES_MIN_SEQUENCE_NUMBER_DESC));

    assertThat(
            groups.stream()
                .mapToLong(RewritePositionDeletesGroup::minDataSequenceNumber)
                .boxed()
                .collect(Collectors.toList()))
        .containsExactly(8L, 5L, 2L);
  }

  @Test
  public void testTaskComparatorFilesAsc() {
    PositionDeletesScanTask task1 = createMockTask(100L, 1L);
    PositionDeletesScanTask task2 = createMockTask(200L, 2L);
    PositionDeletesScanTask task3 = createMockTask(150L, 3L);

    List<PositionDeletesScanTask> tasks = Arrays.asList(task2, task3, task1);
    tasks.sort(RewritePositionDeletesGroup.taskComparator(RewriteJobOrder.FILES_ASC));

    assertThat(
            tasks.stream()
                .mapToLong(t -> t.file().fileSizeInBytes())
                .boxed()
                .collect(Collectors.toList()))
        .containsExactly(100L, 150L, 200L);
  }

  @Test
  public void testTaskComparatorFilesDesc() {
    PositionDeletesScanTask task1 = createMockTask(100L, 1L);
    PositionDeletesScanTask task2 = createMockTask(200L, 2L);
    PositionDeletesScanTask task3 = createMockTask(150L, 3L);

    List<PositionDeletesScanTask> tasks = Arrays.asList(task2, task3, task1);
    tasks.sort(RewritePositionDeletesGroup.taskComparator(RewriteJobOrder.FILES_DESC));

    assertThat(
            tasks.stream()
                .mapToLong(t -> t.file().fileSizeInBytes())
                .boxed()
                .collect(Collectors.toList()))
        .containsExactly(200L, 150L, 100L);
  }

  @Test
  public void testTaskComparatorFilesMinSequenceNumberAsc() {
    PositionDeletesScanTask task1 = createMockTask(100L, 5L);
    PositionDeletesScanTask task2 = createMockTask(200L, 2L);
    PositionDeletesScanTask task3 = createMockTask(150L, 8L);

    List<PositionDeletesScanTask> tasks = Arrays.asList(task1, task3, task2);
    tasks.sort(
        RewritePositionDeletesGroup.taskComparator(RewriteJobOrder.FILES_MIN_SEQUENCE_NUMBER_ASC));

    assertThat(
            tasks.stream()
                .mapToLong(t -> t.file().dataSequenceNumber())
                .boxed()
                .collect(Collectors.toList()))
        .containsExactly(2L, 5L, 8L);
  }

  @Test
  public void testTaskComparatorFilesMinSequenceNumberDesc() {
    PositionDeletesScanTask task1 = createMockTask(100L, 5L);
    PositionDeletesScanTask task2 = createMockTask(200L, 2L);
    PositionDeletesScanTask task3 = createMockTask(150L, 8L);

    List<PositionDeletesScanTask> tasks = Arrays.asList(task1, task3, task2);
    tasks.sort(
        RewritePositionDeletesGroup.taskComparator(RewriteJobOrder.FILES_MIN_SEQUENCE_NUMBER_DESC));

    assertThat(
            tasks.stream()
                .mapToLong(t -> t.file().dataSequenceNumber())
                .boxed()
                .collect(Collectors.toList()))
        .containsExactly(8L, 5L, 2L);
  }

  @Test
  public void testComparatorNone() {
    RewritePositionDeletesGroup group1 = createMockGroup(100L, 1, 5L);
    RewritePositionDeletesGroup group2 = createMockGroup(200L, 2, 2L);
    RewritePositionDeletesGroup group3 = createMockGroup(150L, 3, 8L);

    List<RewritePositionDeletesGroup> groups = Arrays.asList(group1, group3, group2);
    List<RewritePositionDeletesGroup> original = Arrays.asList(group1, group3, group2);
    groups.sort(RewritePositionDeletesGroup.comparator(RewriteJobOrder.NONE));

    // Order should remain unchanged
    assertThat(groups).isEqualTo(original);
  }

  private RewritePositionDeletesGroup createMockGroup(
      long totalBytes, int numFiles, long minSequenceNumber) {
    List<PositionDeletesScanTask> tasks = new java.util.ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      PositionDeletesScanTask task = createMockTask(totalBytes / numFiles, minSequenceNumber + i);
      tasks.add(task);
    }

    RewritePositionDeleteFiles.FileGroupInfo info =
        mock(RewritePositionDeleteFiles.FileGroupInfo.class);
    return new RewritePositionDeletesGroup(info, tasks);
  }

  private PositionDeletesScanTask createMockTask(long fileSize, long sequenceNumber) {
    PositionDeletesScanTask task = mock(PositionDeletesScanTask.class);
    DeleteFile file = mock(DeleteFile.class);
    when(task.file()).thenReturn(file);
    when(task.length()).thenReturn(fileSize);
    when(task.sizeBytes()).thenReturn(fileSize);
    when(file.fileSizeInBytes()).thenReturn(fileSize);
    when(file.dataSequenceNumber()).thenReturn(sequenceNumber);
    return task;
  }
}
