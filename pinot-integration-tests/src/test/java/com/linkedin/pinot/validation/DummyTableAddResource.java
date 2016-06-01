/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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


public class DummyTableAddResource extends ServerResource{
  public static final Logger LOGGER = LoggerFactory.getLogger(DummyTableAddResource.class);
  @Override
  @HttpVerb("get")
  @Description("Receives the command to create a dummy table")
  @Summary("Receives the command  to create a summy table")
  @Paths({ "/AddTable"})
  protected Representation get() throws ResourceException {

    LLRealtimeSegmentManager.getInstance().addTable(ValidationConstants.REALTIME_TABLE_NAME, ValidationConstants.generateInstanceNames());
    return new StringRepresentation("OK");
  }
}
