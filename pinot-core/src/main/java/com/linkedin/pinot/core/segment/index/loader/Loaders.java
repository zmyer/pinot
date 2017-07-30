/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.index.loader;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.column.ColumnIndexContainer;
import com.linkedin.pinot.core.segment.index.converter.SegmentFormatConverter;
import com.linkedin.pinot.core.segment.index.converter.SegmentFormatConverterFactory;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import com.linkedin.pinot.core.segment.store.SegmentDirectoryPaths;
import com.linkedin.pinot.core.startree.StarTreeInterf;
import com.linkedin.pinot.core.startree.StarTreeSerDe;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Loaders {
  private static final Logger LOGGER = LoggerFactory.getLogger(Loaders.class);

  public static class IndexSegment {

    /**
     * For tests only.
     */
    public static com.linkedin.pinot.core.indexsegment.IndexSegment load(@Nonnull File indexDir,
        @Nonnull ReadMode readMode)
        throws Exception {
      IndexLoadingConfig defaultIndexLoadingConfig = new IndexLoadingConfig();
      defaultIndexLoadingConfig.setReadMode(readMode);
      return load(indexDir, defaultIndexLoadingConfig, null);
    }

    /**
     * For tests only.
     */
    public static com.linkedin.pinot.core.indexsegment.IndexSegment load(@Nonnull File indexDir,
        @Nonnull IndexLoadingConfig indexLoadingConfig)
        throws Exception {
      return load(indexDir, indexLoadingConfig, null);
    }

    public static com.linkedin.pinot.core.indexsegment.IndexSegment load(@Nonnull File indexDir,
        @Nonnull IndexLoadingConfig indexLoadingConfig, @Nullable Schema schema)
        throws Exception {
      Preconditions.checkNotNull(indexDir);
      Preconditions.checkArgument(indexDir.exists(), "Index directory: {} does not exist", indexDir);
      Preconditions.checkArgument(indexDir.isDirectory(), "Index directory: {} is not a directory", indexDir);

      ReadMode readMode = indexLoadingConfig.getReadMode();
      SegmentVersion segmentVersionToLoad = indexLoadingConfig.getSegmentVersion();
      StarTreeSerDe.convertStarTreeFormatIfNeeded(indexDir, indexLoadingConfig.getStarTreeVersion());

      String segmentName = indexDir.getName();
      if (!targetFormatAlreadyExists(indexDir, segmentVersionToLoad)) {
        SegmentVersion segmentVersionOnDisk = new SegmentMetadataImpl(indexDir).getSegmentVersion();
        if (segmentVersionOnDisk != segmentVersionToLoad) {
          LOGGER.info("Segment: {} needs to be converted from version: {} to {}", segmentName, segmentVersionOnDisk,
              segmentVersionToLoad);
          SegmentFormatConverter converter =
              SegmentFormatConverterFactory.getConverter(segmentVersionOnDisk, segmentVersionToLoad);
          LOGGER.info("Using converter: {} to up-convert segment: {}", converter.getClass().getName(), segmentName);
          converter.convert(indexDir);
          LOGGER.info("Successfully up-converted segment: {} from version: {} to {}", segmentName, segmentVersionOnDisk,
              segmentVersionToLoad);
        }
      }

      // Add inverted indexes
      // Add, remove or replace default columns
      // Add column min/max values
      // NOTE: this step may modify the segment metadata.
      try (SegmentPreProcessor preProcessor = new SegmentPreProcessor(indexDir, indexLoadingConfig, schema)) {
        preProcessor.process();
      }

      // Load the metadata again since converter and pre-processor may have changed it
      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir, segmentVersionToLoad);
      File segmentDirectoryPath = SegmentDirectoryPaths.segmentDirectoryFor(indexDir, segmentVersionToLoad);
      SegmentDirectory segmentDirectory =
          SegmentDirectory.createFromLocalFS(segmentDirectoryPath, segmentMetadata, readMode);
      SegmentDirectory.Reader segmentReader = segmentDirectory.createReader();
      Map<String, ColumnIndexContainer> indexContainerMap = new HashMap<>();
      for (String column : segmentMetadata.getColumnMetadataMap().keySet()) {
        indexContainerMap.put(column,
            ColumnIndexContainer.init(segmentReader, segmentMetadata.getColumnMetadataFor(column), indexLoadingConfig));
      }

      // Load star tree index if it exists
      StarTreeInterf starTree = null;
      if (segmentReader.hasStarTree()) {
        LOGGER.info("Loading star tree for segment: {}", segmentName);
        starTree = StarTreeSerDe.fromFile(segmentReader.getStarTreeFile(), readMode);
      }

      return new IndexSegmentImpl(segmentDirectory, segmentMetadata, indexContainerMap, starTree);
    }

    private static boolean targetFormatAlreadyExists(File indexDir, SegmentVersion expectedSegmentVersion) {
      return SegmentDirectoryPaths.segmentDirectoryFor(indexDir, expectedSegmentVersion).exists();
    }
  }
}
