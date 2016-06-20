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

package com.linkedin.pinot.validation;

import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.ResourceException;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.restlet.swagger.Description;
import com.linkedin.pinot.common.restlet.swagger.HttpVerb;
import com.linkedin.pinot.common.restlet.swagger.Paths;
import com.linkedin.pinot.common.restlet.swagger.Summary;


// TODO Need to change this to a POST for the real one.
public class SegmentCommitResource extends ServerResource {
  public static final Logger LOGGER = LoggerFactory.getLogger(SegmentCommitResource.class);
  @Override
  @HttpVerb("get")
  @Description("Receives the consumed offset for a partition to be committed")
  @Summary("Receives the consumed offset for a partition to be committed")
  @Paths({ "/" + SegmentFinalProtocol.MSG_TYPE_COMMMIT })
  protected Representation get() throws ResourceException {
    final String offset = getReference().getQueryAsForm().getValues(SegmentFinalProtocol.PARAM_OFFSET);
    final String segmentName = getReference().getQueryAsForm().getValues(SegmentFinalProtocol.PARAM_SEGMENT_NAME);
    final String instanceId = getReference().getQueryAsForm().getValues(SegmentFinalProtocol.PARAM_INSTANCE_ID);
    if (offset == null || segmentName == null || instanceId == null) {
      return new StringRepresentation(SegmentFinalProtocol.RESP_FAILED.toJsonString());
    }
    LOGGER.info("segment={} offset={} instance={} ", segmentName, offset, instanceId);
    SegmentFinalProtocol.Response response = SegmentFinalizer.getInstance().segmentCommit(segmentName, instanceId, Long.valueOf(offset));
    LOGGER.info("Response: instance={}  segment={} status={} offset={}", instanceId, segmentName, response.getStatus(), response.getOffset());
    return new StringRepresentation(response.toJsonString());
  }
}
