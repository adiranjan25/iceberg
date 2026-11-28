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

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;

/**
 * Prototype FileIO wrapper for canonical-to-physical URI resolution.
 *
 * <p>Prefix listing preserves the requested namespace. This wrapper does not establish equivalence
 * between canonical and physical paths for orphan-file detection. It must not be used for
 * orphan-file cleanup until mixed-namespace comparison semantics are defined.
 */
public class CanonicalUriFileIO implements DelegateFileIO {

  private final DelegateFileIO delegate;
  private final String canonicalPrefix;
  private final String physicalPrefix;

  public CanonicalUriFileIO(
      DelegateFileIO delegate, String canonicalPrefix, String physicalPrefix) {
    if (canonicalPrefix == null || canonicalPrefix.isEmpty()) {
      throw new IllegalArgumentException("Canonical URI prefix must not be null or empty");
    }

    if (physicalPrefix == null || physicalPrefix.isEmpty()) {
      throw new IllegalArgumentException("Physical URI prefix must not be null or empty");
    }

    if (!canonicalPrefix.endsWith("/") || !physicalPrefix.endsWith("/")) {
      throw new IllegalArgumentException("URI mapping prefixes must end with '/'");
    }

    String normalizedCanonical = normalizeForMatching(canonicalPrefix);
    String normalizedPhysical = normalizeForMatching(physicalPrefix);

    if (normalizedCanonical.startsWith(normalizedPhysical)
        || normalizedPhysical.startsWith(normalizedCanonical)) {
      throw new IllegalArgumentException("Canonical and physical URI prefixes must not overlap");
    }

    this.delegate = java.util.Objects.requireNonNull(delegate, "Delegate FileIO must not be null");
    this.canonicalPrefix = canonicalPrefix;
    this.physicalPrefix = physicalPrefix;
  }

  private static String normalizeForMatching(String location) {
    int schemeEnd = location.indexOf(':');

    if (org.apache.iceberg.util.LocationUtil.hasScheme(location)
        && location.startsWith(":///", schemeEnd)) {
      return location.substring(0, schemeEnd + 1) + location.substring(schemeEnd + 3);
    }

    return location;
  }

  private static String translate(String location, String sourcePrefix, String targetPrefix) {
    String normalizedLocation = normalizeForMatching(location);
    String normalizedPrefix = normalizeForMatching(sourcePrefix);

    if (normalizedLocation.startsWith(normalizedPrefix)) {
      return targetPrefix + normalizedLocation.substring(normalizedPrefix.length());
    }

    return location;
  }

  private String resolve(String location) {
    return translate(location, canonicalPrefix, physicalPrefix);
  }

  @Override
  public InputFile newInputFile(String location) {
    return wrapInput(delegate.newInputFile(resolve(location)), location);
  }

  @Override
  public InputFile newInputFile(String location, long length) {
    return wrapInput(delegate.newInputFile(resolve(location), length), location);
  }

  private InputFile wrapInput(InputFile physical, String canonical) {
    return new InputFile() {
      @Override
      public long getLength() {
        return physical.getLength();
      }

      @Override
      public SeekableInputStream newStream() {
        return physical.newStream();
      }

      @Override
      public String location() {
        return canonical;
      }

      @Override
      public boolean exists() {
        return physical.exists();
      }
    };
  }

  @Override
  public OutputFile newOutputFile(String location) {
    OutputFile physical = delegate.newOutputFile(resolve(location));

    return new OutputFile() {
      @Override
      public PositionOutputStream create() {
        return physical.create();
      }

      @Override
      public PositionOutputStream createOrOverwrite() {
        return physical.createOrOverwrite();
      }

      @Override
      public String location() {
        return location;
      }

      @Override
      public InputFile toInputFile() {
        return wrapInput(physical.toInputFile(), location);
      }
    };
  }

  @Override
  public void deleteFile(String location) {
    delegate.deleteFile(resolve(location));
  }

  private String canonicalize(String location) {
    return translate(location, physicalPrefix, canonicalPrefix);
  }

  @Override
  public void deleteFiles(Iterable<String> pathsToDelete) throws BulkDeletionFailureException {
    delegate.deleteFiles(Iterables.transform(pathsToDelete, this::resolve));
  }

  @Override
  public Iterable<FileInfo> listPrefix(String prefix) {
    boolean canonicalRequest =
        normalizeForMatching(prefix).startsWith(normalizeForMatching(canonicalPrefix));

    Iterable<FileInfo> files = delegate.listPrefix(resolve(prefix));

    if (!canonicalRequest) {
      return files;
    }

    return Iterables.transform(
        files,
        file -> new FileInfo(canonicalize(file.location()), file.size(), file.createdAtMillis()));
  }

  @Override
  public void deletePrefix(String prefix) {
    delegate.deletePrefix(resolve(prefix));
  }

  @Override
  public Map<String, String> properties() {
    throw new UnsupportedOperationException(
        "Canonical URI configuration serialization is not supported");
  }

  @Override
  public void close() {
    delegate.close();
  }
}
