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

import com.alibaba.fastjson.JSONObject;


public class SegmentFinalProtocol {
  public enum ControllerResponseStatus {
    NOT_SENT, // Never used by the controller. Only used locally by the server
    COMMIT,
    HOLD,
    CATCHUP,
    DISCARD,
    KEEP,
    NOT_LEADER,
    FAILED,
    COMMIT_SUCCESS,
  }
  public static final String STATUS_KEY = "status";
  public static final String OFFSET_KEY = "offset";

  public static final String MSG_TYPE_CONSUMED = "segmentConsumed";
  public static final String MSG_TYPE_COMMMIT = "segmentCommit";

  public static final String PARAM_SEGMENT_NAME = "name";
  public static final String PARAM_OFFSET = "offset";
  public static final String PARAM_INSTANCE_ID = "instance";

  // Canned responses
  public static final Response RESP_NOT_LEADER = new Response(ControllerResponseStatus.NOT_LEADER, -1L);
  public static final Response RESP_FAILED = new Response(ControllerResponseStatus.FAILED, -1L);
  public static final Response RESP_DISCARD = new Response(ControllerResponseStatus.DISCARD, -1L);
  public static final Response RESP_COMMIT_SUCCESS = new Response(ControllerResponseStatus.COMMIT_SUCCESS, -1L);

  public static abstract class Request {
    final String _segmentName;
    final long _offset;
    final String _instanceId;

    public Request(String segmentName, long offset, String instanceId) {
      _segmentName = segmentName;
      _instanceId = instanceId;
      _offset = offset;
    }

    public abstract String getUrl(String hostPort);

    protected String getUri(String method) {
      return "/" + method + "?" +
          PARAM_SEGMENT_NAME + "=" + _segmentName + "&" +
          PARAM_OFFSET + "=" + _offset + "&" +
          PARAM_INSTANCE_ID + "=" + _instanceId;
    }
  }

  public static class SegmentConsumedRequest extends Request {
    public SegmentConsumedRequest(String segmentName, long offset, String instanceId) {
      super(segmentName, offset, instanceId);
    }
    @Override
    public String getUrl(final String hostPort) {
      return "http://" + hostPort + getUri(MSG_TYPE_CONSUMED);
    }
  }

  public static class SegmentCommitRequest extends Request {
    public SegmentCommitRequest(String segmentName, long offset, String instanceId) {
      super(segmentName, offset, instanceId);
    }
    @Override
    public String getUrl(final String hostPort) {
      return "http://" + hostPort + getUri(MSG_TYPE_COMMMIT);
    }
  }

  public static class Response {
    final ControllerResponseStatus _status;
    final long _offset;

    public Response(String jsonRespStr) {
      JSONObject jsonObject = JSONObject.parseObject(jsonRespStr);
      long offset = -1;
      if (jsonObject.containsKey(OFFSET_KEY)) {
        offset = jsonObject.getLong(OFFSET_KEY);
      }
      _offset = offset;

      String statusStr = jsonObject.getString(STATUS_KEY);
      ControllerResponseStatus status;
      try {
        status = ControllerResponseStatus.valueOf(statusStr);
      } catch (Exception e) {
        status = ControllerResponseStatus.FAILED;
      }
      _status = status;
    }

    public Response(ControllerResponseStatus status, long offset) {
      _status = status;
      _offset = offset;
    }

    public ControllerResponseStatus getStatus() {
      return _status;
    }

    public long getOffset() {
      return _offset;
    }

    public String toJsonString() {
      return "{\"" + STATUS_KEY + "\":" + "\"" + _status.name() + "\"," +
          "\"" + OFFSET_KEY + "\":" + _offset + "}";
    }
  }
}
