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
package com.linkedin.pinot.controller.api.restlet.resources;

import com.linkedin.pinot.common.protocols.SegmentCompletionProtocol;
import com.linkedin.pinot.common.restlet.swagger.Description;
import com.linkedin.pinot.common.restlet.swagger.HttpVerb;
import com.linkedin.pinot.common.restlet.swagger.Paths;
import com.linkedin.pinot.common.restlet.swagger.Summary;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.controller.helix.core.realtime.SegmentCompletionManager;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.io.FileUtils;
import org.restlet.ext.fileupload.RestletFileUpload;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A resource class that fields the segmentCommit() message (and the upload of a realtime segment)
 * from the servers. Servers go through {@link SegmentCompletionProtocol} to complete a segment and
 * one of the servers commit the segment that has been completed.
 */
public class LLCSegmentCommit extends PinotSegmentUploadRestletResource {
  private static Logger LOGGER = LoggerFactory.getLogger(LLCSegmentCommit.class);

  public LLCSegmentCommit()
      throws IOException {
  }

  @Override
  @HttpVerb("post")
  @Description("Uploads an LLC segment coming in from a server")
  @Summary("Uploads an LLC segment coming in from a server")
  @Paths({"/" + SegmentCompletionProtocol.MSG_TYPE_COMMIT})
  public Representation post(Representation entity) {
    SegmentCompletionProtocol.Request.Params requestParams = SegmentCompletionUtils.extractParams(getReference());
    if (requestParams == null) {
      return new StringRepresentation(SegmentCompletionProtocol.RESP_FAILED.toJsonString());
    }
    LOGGER.info(requestParams.toString());
    final SegmentCompletionManager segmentCompletionManager = getSegmentCompletionManager();
    SegmentCompletionProtocol.Response response = segmentCompletionManager.segmentCommitStart(requestParams);
    if (response.equals(SegmentCompletionProtocol.RESP_COMMIT_CONTINUE)) {

      // Get the segment and put it in the right place.
      boolean success = uploadSegment(requestParams.getInstanceId(), requestParams.getSegmentName());

      response = segmentCompletionManager.segmentCommitEnd(requestParams, success, false);
    }

    LOGGER.info("Response: instance={}  segment={} status={} offset={}", requestParams.getInstanceId(), requestParams.getSegmentName(),
        response.getStatus(), response.getOffset());
    return new StringRepresentation(response.toJsonString());
  }

  boolean uploadSegment(final String instanceId, final String segmentNameStr) {
    // 1/ Create a factory for disk-based file items
    final DiskFileItemFactory factory = new DiskFileItemFactory();

    // 2/ Create a new file upload handler based on the Restlet
    // FileUpload extension that will parse Restlet requests and
    // generates FileItems.
    final RestletFileUpload upload = new RestletFileUpload(factory);
    final List<FileItem> items;

    try {
      // The following statement blocks until the entire segment is read into memory/disk.
      items = upload.parseRequest(getRequest());

      File dataFile = null;

      // TODO: refactor this part into a util method (almost duplicate code in PinotSegmentUploadRestletResource and
      // PinotSchemaRestletResource)
      for (FileItem fileItem : items) {
        String fieldName = fileItem.getFieldName();
        if (dataFile == null) {
          if (fieldName != null && fieldName.equals(segmentNameStr)) {
            dataFile = new File(tempDir, fieldName);
            fileItem.write(dataFile);
          } else {
            LOGGER.warn("Invalid field name: {}", fieldName);
          }
        } else {
          LOGGER.warn("Got extra file item while uploading LLC segments: {}", fieldName);
        }

        // Remove the temp file
        // When the file is copied to instead of renamed to the new file, the temp file might be left in the dir
        fileItem.delete();
      }

      if (dataFile == null) {
        LOGGER.error("Segment not included in request. Instance {}, segment {}", instanceId, segmentNameStr);
        return false;
      }
      // We will not check for quota here. Instead, committed segments will count towards the quota of a
      // table
      LLCSegmentName segmentName = new LLCSegmentName(segmentNameStr);
      final String rawTableName = segmentName.getTableName();
      final File tableDir = new File(baseDataDir, rawTableName);
      final File segmentFile = new File(tableDir, segmentNameStr);

      synchronized (_pinotHelixResourceManager) {
        if (segmentFile.exists()) {
          LOGGER.warn("Segment file {} exists. Replacing with upload from {}", segmentNameStr, instanceId);
          FileUtils.deleteQuietly(segmentFile);
        }
        FileUtils.moveFile(dataFile, segmentFile);
      }

      return true;
    } catch (Exception e) {
      LOGGER.error("File upload exception from instance {} for segment {}", instanceId, segmentNameStr, e);
    }
    return false;
  }
}
