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
package org.apache.iceberg.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class TestCanonicalUriFileIO {

  @Test
  public void testCanonicalLocationContract() {
    String canonical = "storage://warehouse/db/table/data.parquet";
    String physical = "s3://warehouse/db/table/data.parquet";

    PrefixTestFileIO delegate = new PrefixTestFileIO();

    CanonicalUriFileIO io =
        new CanonicalUriFileIO(delegate, "storage://warehouse/", "s3://warehouse/");

    OutputFile output = io.newOutputFile(canonical);

    assertThat(output.location()).isEqualTo(canonical);

    try (PositionOutputStream stream = output.create()) {
      stream.write(42);
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }

    assertThat(output.toInputFile().location()).isEqualTo(canonical);
    assertThat(io.newInputFile(canonical).location()).isEqualTo(canonical);

    assertThat(delegate.fileExists(physical)).isTrue();
    assertThat(delegate.fileExists(canonical)).isFalse();

    try (SeekableInputStream stream = io.newInputFile(canonical).newStream()) {
      assertThat(stream.read()).isEqualTo(42);
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }

    io.deleteFile(canonical);
    assertThat(delegate.fileExists(physical)).isFalse();
  }

  @Test
  public void testUnmappedPhysicalLocation() {
    PrefixTestFileIO delegate = new PrefixTestFileIO();

    CanonicalUriFileIO io =
        new CanonicalUriFileIO(delegate, "storage://warehouse/", "s3://warehouse/");

    String physical = "s3://warehouse/existing.parquet";
    delegate.addFile(physical, new byte[] {42});

    assertThat(io.newInputFile(physical).location()).isEqualTo(physical);
    assertThat(io.newInputFile(physical).exists()).isTrue();
  }

  @Test
  public void testInvalidUriMappingConfiguration() {
    PrefixTestFileIO delegate = new PrefixTestFileIO();

    assertThatThrownBy(() -> new CanonicalUriFileIO(delegate, "", "s3://warehouse/"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Canonical URI prefix must not be null or empty");

    assertThatThrownBy(() -> new CanonicalUriFileIO(delegate, "storage://warehouse/", ""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Physical URI prefix must not be null or empty");

    assertThatThrownBy(() -> new CanonicalUriFileIO(delegate, null, "s3://warehouse/"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Canonical URI prefix must not be null or empty");

    assertThatThrownBy(() -> new CanonicalUriFileIO(delegate, "storage://warehouse/", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Physical URI prefix must not be null or empty");
  }

  @Test
  public void testCanonicalPrefixBoundary() {
    PrefixTestFileIO delegate = new PrefixTestFileIO();

    CanonicalUriFileIO io =
        new CanonicalUriFileIO(delegate, "storage://warehouse/", "s3://warehouse/");

    String canonical = "storage://warehouse/data.parquet";
    String similar = "storage://warehouse-other/data.parquet";

    try (PositionOutputStream stream = io.newOutputFile(canonical).create()) {
      stream.write(42);
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }

    assertThat(delegate.fileExists("s3://warehouse/data.parquet")).isTrue();

    try (PositionOutputStream stream = io.newOutputFile(similar).create()) {
      stream.write(43);
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }

    assertThat(delegate.fileExists(similar)).isTrue();
    assertThat(delegate.fileExists("s3://warehouse-other/data.parquet")).isFalse();
  }

  @Test
  public void testUriPrefixesRequireTrailingSlash() {
    PrefixTestFileIO delegate = new PrefixTestFileIO();

    assertThatThrownBy(
            () -> new CanonicalUriFileIO(delegate, "storage://warehouse", "s3://warehouse/"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must end with");

    assertThatThrownBy(
            () -> new CanonicalUriFileIO(delegate, "storage://warehouse/", "s3://warehouse"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must end with");
  }

  @Test
  public void testBulkAndPrefixOperations() {
    PrefixTestFileIO delegate = new PrefixTestFileIO();

    CanonicalUriFileIO io =
        new CanonicalUriFileIO(delegate, "storage://warehouse/", "s3://warehouse/");

    String first = "storage://warehouse/table/first.parquet";
    String second = "storage://warehouse/table/second.parquet";
    String third = "storage://warehouse/table/third.parquet";
    String physicalFirst = "s3://warehouse/table/first.parquet";
    String physicalSecond = "s3://warehouse/table/second.parquet";
    String physicalThird = "s3://warehouse/table/third.parquet";

    delegate.addFile(physicalFirst, new byte[] {1});
    delegate.addFile(physicalSecond, new byte[] {2});
    delegate.addFile(physicalThird, new byte[] {3});

    assertThat(io.listPrefix("storage://warehouse/table/"))
        .extracting(FileInfo::location)
        .containsExactlyInAnyOrder(first, second, third);

    io.deleteFiles(java.util.Arrays.asList(first, second));

    assertThat(delegate.fileExists(physicalFirst)).isFalse();
    assertThat(delegate.fileExists(physicalSecond)).isFalse();
    assertThat(delegate.fileExists(physicalThird)).isTrue();

    io.deletePrefix("storage://warehouse/table/");

    assertThat(delegate.fileExists(physicalThird)).isFalse();
  }

  private static class PrefixTestFileIO extends org.apache.iceberg.inmemory.InMemoryFileIO
      implements DelegateFileIO {

    private final java.util.List<String> paths = new java.util.ArrayList<>();

    @Override
    public void addFile(String location, byte[] contents) {
      super.addFile(location, contents);
      paths.add(location);
    }

    @Override
    public Iterable<FileInfo> listPrefix(String prefix) {
      java.util.List<FileInfo> files = new java.util.ArrayList<>();
      for (String path : paths) {
        if (path.startsWith(prefix) && fileExists(path)) {
          files.add(new FileInfo(path, newInputFile(path).getLength(), 0L));
        }
      }
      return files;
    }

    @Override
    public void deleteFiles(Iterable<String> pathsToDelete) throws BulkDeletionFailureException {
      int failures = 0;

      for (String path : pathsToDelete) {
        try {
          deleteFile(path);
        } catch (RuntimeException e) {
          failures += 1;
        }
      }

      if (failures > 0) {
        throw new BulkDeletionFailureException(failures);
      }
    }

    @Override
    public void deletePrefix(String prefix) {
      for (String path : paths) {
        if (path.startsWith(prefix) && fileExists(path)) {
          deleteFile(path);
        }
      }
    }
  }

  @Test
  public void testUnsupportedConfigurationSerialization() {
    CanonicalUriFileIO io =
        new CanonicalUriFileIO(new PrefixTestFileIO(), "storage://warehouse/", "s3://warehouse/");

    assertThatThrownBy(io::properties)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("configuration serialization is not supported");

    assertThatThrownBy(() -> FileIOParser.toJson(io))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot serialize FileIO");
  }

  @Test
  public void testLazyPrefixListing() {
    java.util.concurrent.atomic.AtomicInteger consumed =
        new java.util.concurrent.atomic.AtomicInteger();

    PrefixTestFileIO delegate =
        new PrefixTestFileIO() {
          @Override
          public Iterable<FileInfo> listPrefix(String prefix) {
            return org.apache.iceberg.relocated.com.google.common.collect.Iterables.transform(
                java.util.List.of(
                    new FileInfo("s3://warehouse/table/first.parquet", 10L, 20L),
                    new FileInfo("s3://warehouse/table/second.parquet", 30L, 40L)),
                file -> {
                  consumed.incrementAndGet();
                  return file;
                });
          }
        };

    CanonicalUriFileIO io =
        new CanonicalUriFileIO(delegate, "storage://warehouse/", "s3://warehouse/");

    Iterable<FileInfo> files = io.listPrefix("storage://warehouse/table/");

    assertThat(consumed.get()).isZero();

    java.util.Iterator<FileInfo> iterator = files.iterator();
    FileInfo first = iterator.next();

    assertThat(consumed.get()).isEqualTo(1);
    assertThat(first.location()).isEqualTo("storage://warehouse/table/first.parquet");
    assertThat(first.size()).isEqualTo(10L);
    assertThat(first.createdAtMillis()).isEqualTo(20L);

    assertThat(iterator.next().location()).isEqualTo("storage://warehouse/table/second.parquet");
    assertThat(consumed.get()).isEqualTo(2);
    assertThat(iterator.hasNext()).isFalse();
  }

  @Test
  public void testBulkDeletionFailurePropagation() {
    DelegateFileIO delegate = org.mockito.Mockito.mock(DelegateFileIO.class);

    BulkDeletionFailureException failure = new BulkDeletionFailureException(2);

    org.mockito.Mockito.doThrow(failure)
        .when(delegate)
        .deleteFiles(org.mockito.ArgumentMatchers.any());

    CanonicalUriFileIO io =
        new CanonicalUriFileIO(delegate, "storage://warehouse/", "s3://warehouse/");

    assertThatThrownBy(
            () ->
                io.deleteFiles(
                    java.util.List.of(
                        "storage://warehouse/first.parquet", "storage://warehouse/second.parquet")))
        .isSameAs(failure);
  }

  @Test
  public void testOverlappingUriPrefixes() {
    PrefixTestFileIO delegate = new PrefixTestFileIO();

    assertThatThrownBy(
            () -> new CanonicalUriFileIO(delegate, "s3://warehouse/", "s3://warehouse/data/"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must not overlap");

    assertThatThrownBy(
            () -> new CanonicalUriFileIO(delegate, "s3://warehouse/data/", "s3://warehouse/"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must not overlap");

    assertThatThrownBy(() -> new CanonicalUriFileIO(delegate, "s3://warehouse/", "s3://warehouse/"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must not overlap");

    assertThatThrownBy(
            () ->
                new CanonicalUriFileIO(
                    delegate, "file:///tmp/warehouse/", "file:/tmp/warehouse/data/"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must not overlap");
  }

  @Test
  public void testEquivalentEmptyAuthorityUris() {
    PrefixTestFileIO delegate = new PrefixTestFileIO();

    CanonicalUriFileIO io =
        new CanonicalUriFileIO(delegate, "storage://warehouse/", "file:///tmp/warehouse/");

    delegate.addFile("file:/tmp/warehouse/data.parquet", new byte[] {42});

    assertThat(io.listPrefix("storage://warehouse/")).isEmpty();

    DelegateFileIO listingDelegate = org.mockito.Mockito.mock(DelegateFileIO.class);

    org.mockito.Mockito.when(listingDelegate.listPrefix("file:///tmp/warehouse/"))
        .thenReturn(java.util.List.of(new FileInfo("file:/tmp/warehouse/data.parquet", 1L, 0L)));

    CanonicalUriFileIO listingIO =
        new CanonicalUriFileIO(listingDelegate, "storage://warehouse/", "file:///tmp/warehouse/");

    assertThat(listingIO.listPrefix("storage://warehouse/"))
        .extracting(FileInfo::location)
        .containsExactly("storage://warehouse/data.parquet");
  }

  @Test
  public void testPreserveRawObjectKeys() {
    PrefixTestFileIO delegate = new PrefixTestFileIO();

    CanonicalUriFileIO io =
        new CanonicalUriFileIO(delegate, "storage://warehouse/", "s3://warehouse/");

    String suffix = "partition=a%2F..%2Fb/data [1]%.parquet";

    delegate.addFile("s3://warehouse/" + suffix, new byte[] {42});

    assertThat(io.newInputFile("storage://warehouse/" + suffix).exists()).isTrue();
  }

  @Test
  public void testNonOverlappingSiblingPrefixes() {
    PrefixTestFileIO delegate = new PrefixTestFileIO();

    CanonicalUriFileIO io =
        new CanonicalUriFileIO(delegate, "s3://warehouse/data/", "s3://warehouse/data-other/");

    delegate.addFile("s3://warehouse/data-other/file.parquet", new byte[] {42});

    assertThat(io.newInputFile("s3://warehouse/data/file.parquet").exists()).isTrue();
  }

  @Test
  public void testPhysicalListingPreservesLocations() {
    PrefixTestFileIO delegate = new PrefixTestFileIO();

    String physical = "s3://warehouse/table/live.parquet";
    String canonical = "storage://warehouse/table/live.parquet";

    delegate.addFile(physical, new byte[] {42});

    CanonicalUriFileIO io =
        new CanonicalUriFileIO(delegate, "storage://warehouse/", "s3://warehouse/");

    assertThat(io.listPrefix("s3://warehouse/table/"))
        .extracting(FileInfo::location)
        .containsExactly(physical);

    assertThat(io.listPrefix("storage://warehouse/table/"))
        .extracting(FileInfo::location)
        .containsExactly(canonical);

    assertThat(delegate.fileExists(physical)).isTrue();
  }

  @Test
  public void testUnmappedListingPreservesLocations() {
    PrefixTestFileIO delegate = new PrefixTestFileIO();

    String unrelated = "s3://other-warehouse/table/live.parquet";
    delegate.addFile(unrelated, new byte[] {42});

    CanonicalUriFileIO io =
        new CanonicalUriFileIO(delegate, "storage://warehouse/", "s3://warehouse/");

    assertThat(io.listPrefix("s3://other-warehouse/table/"))
        .extracting(FileInfo::location)
        .containsExactly(unrelated);
  }

  @Test
  public void testPhysicalParentListingPreservesLocations() {
    PrefixTestFileIO delegate = new PrefixTestFileIO();

    String mapped = "s3://warehouse/data/live.parquet";
    String unmapped = "s3://warehouse/other/live.parquet";

    delegate.addFile(mapped, new byte[] {42});
    delegate.addFile(unmapped, new byte[] {43});

    CanonicalUriFileIO io =
        new CanonicalUriFileIO(delegate, "storage://warehouse/", "s3://warehouse/data/");

    assertThat(io.listPrefix("s3://warehouse/"))
        .extracting(FileInfo::location)
        .containsExactlyInAnyOrder(mapped, unmapped);
  }

  @Test
  public void testRealHadoopFileIOListing(
      @org.junit.jupiter.api.io.TempDir java.nio.file.Path tempDir) throws Exception {
    java.nio.file.Path warehouse =
        java.nio.file.Files.createDirectory(tempDir.resolve("warehouse"));

    java.nio.file.Path dataFile =
        java.nio.file.Files.write(warehouse.resolve("live.parquet"), new byte[] {42});

    String physicalPrefix = warehouse.toUri().toString();
    String canonicalPrefix = "storage://warehouse/";

    org.apache.iceberg.hadoop.HadoopFileIO delegate =
        new org.apache.iceberg.hadoop.HadoopFileIO(new org.apache.hadoop.conf.Configuration());

    try (CanonicalUriFileIO io =
        new CanonicalUriFileIO(delegate, canonicalPrefix, physicalPrefix)) {

      assertThat(io.newInputFile(canonicalPrefix + "live.parquet").exists()).isTrue();

      java.util.List<String> physicalLocations =
          org.apache.iceberg.relocated.com.google.common.collect.Streams.stream(
                  delegate.listPrefix(physicalPrefix))
              .map(FileInfo::location)
              .collect(java.util.stream.Collectors.toList());

      assertThat(physicalLocations).hasSize(1);

      assertThat(io.listPrefix(physicalPrefix))
          .extracting(FileInfo::location)
          .containsExactlyElementsOf(physicalLocations);

      assertThat(io.listPrefix(canonicalPrefix))
          .extracting(FileInfo::location)
          .containsExactly(canonicalPrefix + "live.parquet");

      assertThat(java.nio.file.Files.exists(dataFile)).isTrue();
    }
  }

  @Test
  public void testRealHadoopFileIOEquivalentUris(
      @org.junit.jupiter.api.io.TempDir java.nio.file.Path tempDir) throws Exception {
    java.nio.file.Path warehouse =
        java.nio.file.Files.createDirectory(tempDir.resolve("warehouse"));

    java.nio.file.Files.write(warehouse.resolve("data.parquet"), new byte[] {42});

    String tripleSlash = warehouse.toUri().toString();
    String singleSlash = tripleSlash.replaceFirst("^file:///", "file:/");

    assertThat(tripleSlash).startsWith("file:///");
    assertThat(singleSlash).startsWith("file:/");

    org.apache.iceberg.hadoop.HadoopFileIO delegate =
        new org.apache.iceberg.hadoop.HadoopFileIO(new org.apache.hadoop.conf.Configuration());

    try (CanonicalUriFileIO io =
        new CanonicalUriFileIO(delegate, "storage://warehouse/", singleSlash)) {

      assertThat(io.listPrefix("storage://warehouse/"))
          .extracting(FileInfo::location)
          .containsExactly("storage://warehouse/data.parquet");

      assertThat(io.newInputFile("storage://warehouse/data.parquet").exists()).isTrue();

      assertThat(io.listPrefix(tripleSlash))
          .extracting(FileInfo::location)
          .containsExactlyElementsOf(
              org.apache.iceberg.relocated.com.google.common.collect.Streams.stream(
                      delegate.listPrefix(tripleSlash))
                  .map(FileInfo::location)
                  .collect(java.util.stream.Collectors.toList()));
    }
  }

  @Test
  public void testSerializationRoundTrip() throws Exception {
    CanonicalUriFileIO io =
        new CanonicalUriFileIO(new PrefixTestFileIO(), "storage://warehouse/", "s3://warehouse/");

    java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();

    try (java.io.ObjectOutputStream output = new java.io.ObjectOutputStream(bytes)) {
      output.writeObject(io);
    }

    CanonicalUriFileIO restored;

    try (java.io.ObjectInputStream input =
        new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes.toByteArray()))) {
      restored = (CanonicalUriFileIO) input.readObject();
    }

    assertThat(restored.newOutputFile("storage://warehouse/test.parquet").location())
        .isEqualTo("storage://warehouse/test.parquet");
  }
}
