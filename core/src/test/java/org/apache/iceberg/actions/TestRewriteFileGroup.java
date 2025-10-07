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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteJobOrder;
import org.junit.jupiter.api.Test;

public class TestRewriteFileGroup {

  @Test
  public void testComparatorBytesAsc() {
    RewriteFileGroup group1 = createMockGroup(100L, 1, 1L);
    RewriteFileGroup group2 = createMockGroup(200L, 2, 2L);
    RewriteFileGroup group3 = createMockGroup(150L, 3, 3L);

    List<RewriteFileGroup> groups = Arrays.asList(group2, group3, group1);
    groups.sort(RewriteFileGroup.comparator(RewriteJobOrder.BYTES_ASC));

    assertThat(
            groups.stream()
                .mapToLong(RewriteFileGroup::sizeInBytes)
                .boxed()
                .collect(Collectors.toList()))
        .containsExactly(100L, 150L, 200L);
  }

  @Test
  public void testComparatorBytesDesc() {
    RewriteFileGroup group1 = createMockGroup(100L, 1, 1L);
    RewriteFileGroup group2 = createMockGroup(200L, 2, 2L);
    RewriteFileGroup group3 = createMockGroup(150L, 3, 3L);

    List<RewriteFileGroup> groups = Arrays.asList(group2, group3, group1);
    groups.sort(RewriteFileGroup.comparator(RewriteJobOrder.BYTES_DESC));

    assertThat(
            groups.stream()
                .mapToLong(RewriteFileGroup::sizeInBytes)
                .boxed()
                .collect(Collectors.toList()))
        .containsExactly(200L, 150L, 100L);
  }

  @Test
  public void testComparatorFilesAsc() {
    RewriteFileGroup group1 = createMockGroup(100L, 1, 1L);
    RewriteFileGroup group2 = createMockGroup(200L, 3, 2L);
    RewriteFileGroup group3 = createMockGroup(150L, 2, 3L);

    List<RewriteFileGroup> groups = Arrays.asList(group2, group3, group1);
    groups.sort(RewriteFileGroup.comparator(RewriteJobOrder.FILES_ASC));

    assertThat(
            groups.stream()
                .mapToInt(RewriteFileGroup::numFiles)
                .boxed()
                .collect(Collectors.toList()))
        .containsExactly(1, 2, 3);
  }

  @Test
  public void testComparatorFilesDesc() {
    RewriteFileGroup group1 = createMockGroup(100L, 1, 1L);
    RewriteFileGroup group2 = createMockGroup(200L, 3, 2L);
    RewriteFileGroup group3 = createMockGroup(150L, 2, 3L);

    List<RewriteFileGroup> groups = Arrays.asList(group2, group3, group1);
    groups.sort(RewriteFileGroup.comparator(RewriteJobOrder.FILES_DESC));

    assertThat(
            groups.stream()
                .mapToInt(RewriteFileGroup::numFiles)
                .boxed()
                .collect(Collectors.toList()))
        .containsExactly(3, 2, 1);
  }

  @Test
  public void testComparatorFilesMinSequenceNumberAsc() {
    RewriteFileGroup group1 = createMockGroup(100L, 1, 5L);
    RewriteFileGroup group2 = createMockGroup(200L, 2, 2L);
    RewriteFileGroup group3 = createMockGroup(150L, 3, 8L);

    List<RewriteFileGroup> groups = Arrays.asList(group1, group3, group2);
    groups.sort(RewriteFileGroup.comparator(RewriteJobOrder.FILES_MIN_SEQUENCE_NUMBER_ASC));

    assertThat(
            groups.stream()
                .mapToLong(RewriteFileGroup::minFileSequenceNumber)
                .boxed()
                .collect(Collectors.toList()))
        .containsExactly(2L, 5L, 8L);
  }

  @Test
  public void testComparatorFilesMinSequenceNumberDesc() {
    RewriteFileGroup group1 = createMockGroup(100L, 1, 5L);
    RewriteFileGroup group2 = createMockGroup(200L, 2, 2L);
    RewriteFileGroup group3 = createMockGroup(150L, 3, 8L);

    List<RewriteFileGroup> groups = Arrays.asList(group1, group3, group2);
    groups.sort(RewriteFileGroup.comparator(RewriteJobOrder.FILES_MIN_SEQUENCE_NUMBER_DESC));

    assertThat(
            groups.stream()
                .mapToLong(RewriteFileGroup::minFileSequenceNumber)
                .boxed()
                .collect(Collectors.toList()))
        .containsExactly(8L, 5L, 2L);
  }

  @Test
  public void testTaskComparatorFilesAsc() {
    FileScanTask task1 = createMockTask(100L, 1L);
    FileScanTask task2 = createMockTask(200L, 2L);
    FileScanTask task3 = createMockTask(150L, 3L);

    List<FileScanTask> tasks = Arrays.asList(task2, task3, task1);
    tasks.sort(RewriteFileGroup.taskComparator(RewriteJobOrder.FILES_ASC));

    assertThat(tasks.stream().mapToLong(FileScanTask::length).boxed().collect(Collectors.toList()))
        .containsExactly(100L, 150L, 200L);
  }

  @Test
  public void testTaskComparatorFilesDesc() {
    FileScanTask task1 = createMockTask(100L, 1L);
    FileScanTask task2 = createMockTask(200L, 2L);
    FileScanTask task3 = createMockTask(150L, 3L);

    List<FileScanTask> tasks = Arrays.asList(task2, task3, task1);
    tasks.sort(RewriteFileGroup.taskComparator(RewriteJobOrder.FILES_DESC));

    assertThat(tasks.stream().mapToLong(FileScanTask::length).boxed().collect(Collectors.toList()))
        .containsExactly(200L, 150L, 100L);
  }

  @Test
  public void testTaskComparatorFilesMinSequenceNumberAsc() {
    FileScanTask task1 = createMockTask(100L, 5L);
    FileScanTask task2 = createMockTask(200L, 2L);
    FileScanTask task3 = createMockTask(150L, 8L);

    List<FileScanTask> tasks = Arrays.asList(task1, task3, task2);
    tasks.sort(RewriteFileGroup.taskComparator(RewriteJobOrder.FILES_MIN_SEQUENCE_NUMBER_ASC));

    assertThat(
            tasks.stream()
                .mapToLong(t -> t.file().dataSequenceNumber())
                .boxed()
                .collect(Collectors.toList()))
        .containsExactly(2L, 5L, 8L);
  }

  @Test
  public void testTaskComparatorFilesMinSequenceNumberDesc() {
    FileScanTask task1 = createMockTask(100L, 5L);
    FileScanTask task2 = createMockTask(200L, 2L);
    FileScanTask task3 = createMockTask(150L, 8L);

    List<FileScanTask> tasks = Arrays.asList(task1, task3, task2);
    tasks.sort(RewriteFileGroup.taskComparator(RewriteJobOrder.FILES_MIN_SEQUENCE_NUMBER_DESC));

    assertThat(
            tasks.stream()
                .mapToLong(t -> t.file().dataSequenceNumber())
                .boxed()
                .collect(Collectors.toList()))
        .containsExactly(8L, 5L, 2L);
  }

  @Test
  public void testComparatorNone() {
    RewriteFileGroup group1 = createMockGroup(100L, 1, 5L);
    RewriteFileGroup group2 = createMockGroup(200L, 2, 2L);
    RewriteFileGroup group3 = createMockGroup(150L, 3, 8L);

    List<RewriteFileGroup> groups = Arrays.asList(group1, group3, group2);
    List<RewriteFileGroup> original = Arrays.asList(group1, group3, group2);
    groups.sort(RewriteFileGroup.comparator(RewriteJobOrder.NONE));

    // Order should remain unchanged
    assertThat(groups).isEqualTo(original);
  }

  private RewriteFileGroup createMockGroup(long totalBytes, int numFiles, long minSequenceNumber) {
    List<FileScanTask> tasks = new java.util.ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      FileScanTask task = createMockTask(totalBytes / numFiles, minSequenceNumber + i);
      tasks.add(task);
    }

    RewriteDataFiles.FileGroupInfo info = mock(RewriteDataFiles.FileGroupInfo.class);
    return new RewriteFileGroup(info, tasks);
  }

  private FileScanTask createMockTask(long length, long sequenceNumber) {
    FileScanTask task = mock(FileScanTask.class);
    DataFile file = mock(DataFile.class);
    when(task.length()).thenReturn(length);
    when(task.file()).thenReturn(file);
    when(file.dataSequenceNumber()).thenReturn(sequenceNumber);
    when(file.fileSequenceNumber()).thenReturn(sequenceNumber);
    return task;
  }
}
