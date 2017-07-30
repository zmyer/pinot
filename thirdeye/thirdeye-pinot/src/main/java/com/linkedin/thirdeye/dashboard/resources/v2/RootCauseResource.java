package com.linkedin.thirdeye.dashboard.resources.v2;

import com.linkedin.thirdeye.dashboard.resources.v2.pojo.RootCauseEntity;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.RCAFramework;
import com.linkedin.thirdeye.rootcause.RCAFrameworkExecutionResult;
import com.linkedin.thirdeye.rootcause.impl.EntityUtils;
import com.linkedin.thirdeye.rootcause.impl.TimeRangeEntity;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path(value = "/rootcause")
@Produces(MediaType.APPLICATION_JSON)
public class RootCauseResource {
  private static final Logger LOG = LoggerFactory.getLogger(RootCauseResource.class);

  private final List<RootCauseEntityFormatter> formatters;
  private final Map<String, RCAFramework> frameworks;

  public RootCauseResource(Map<String, RCAFramework> frameworks, List<RootCauseEntityFormatter> formatters) {
    this.frameworks = frameworks;
    this.formatters = formatters;
  }

  @GET
  @Path("/query")
  public List<RootCauseEntity> query(
      @QueryParam("framework") String framework,
      @QueryParam("anomalyStart") Long anomalyStart,
      @QueryParam("anomalyEnd") Long anomalyEnd,
      @QueryParam("baselineStart") Long baselineStart,
      @QueryParam("baselineEnd") Long baselineEnd,
      @QueryParam("analysisStart") Long analysisStart,
      @QueryParam("analysisEnd") Long analysisEnd,
      @QueryParam("urns") List<String> urns) throws Exception {

    // configuration validation
    if(!this.frameworks.containsKey(framework))
      throw new IllegalArgumentException(String.format("Could not resolve framework '%s'", framework));

    // input validation
    if(anomalyStart == null)
      throw new IllegalArgumentException("Must provide anomaly start timestamp (in milliseconds)");

    if(anomalyEnd == null)
      throw new IllegalArgumentException("Must provide anomaly end timestamp (in milliseconds)");

    if(baselineStart == null)
      throw new IllegalArgumentException("Must provide baseline start timestamp (in milliseconds)");

    if(baselineEnd == null)
      throw new IllegalArgumentException("Must provide baseline end timestamp (in milliseconds)");

    if(analysisStart == null)
      throw new IllegalArgumentException("Must provide analysis start timestamp (in milliseconds)");

    if(analysisEnd == null)
      throw new IllegalArgumentException("Must provide analysis end timestamp (in milliseconds)");

    urns = parseUrnsParam(urns);
    if(urns.isEmpty())
      throw new IllegalArgumentException("Must provide entity urns");

    // validate window size
    long anomalyWindow = anomalyEnd - anomalyStart;
    long baselineWindow = baselineEnd - baselineStart;
    if(anomalyWindow != baselineWindow)
      throw new IllegalArgumentException("Must provide equal-sized anomaly and baseline periods");

    // format input
    Set<Entity> input = new HashSet<>();
    input.add(TimeRangeEntity.fromRange(1.0, TimeRangeEntity.TYPE_ANOMALY, anomalyStart, anomalyEnd));
    input.add(TimeRangeEntity.fromRange(0.8, TimeRangeEntity.TYPE_BASELINE, baselineStart, baselineEnd));
    input.add(TimeRangeEntity.fromRange(1.0, TimeRangeEntity.TYPE_ANALYSIS, analysisStart, analysisEnd));
    for(String urn : urns) {
      input.add(EntityUtils.parseURN(urn, 1.0));
    }

    // run root-cause analysis
    RCAFrameworkExecutionResult result = this.frameworks.get(framework).run(input);

    // apply formatters
    return applyFormatters(result.getResultsSorted());
  }

  @GET
  @Path("/raw")
  public List<RootCauseEntity> raw(
      @QueryParam("framework") String framework,
      @QueryParam("urns") List<String> urns) throws Exception {

    // configuration validation
    if(!this.frameworks.containsKey(framework))
      throw new IllegalArgumentException(String.format("Could not resolve framework '%s'", framework));

    // parse urns arg
    urns = parseUrnsParam(urns);

    // format input
    Set<Entity> input = new HashSet<>();
    for(String urn : urns) {
      input.add(EntityUtils.parseURNRaw(urn, 1.0));
    }

    // run root-cause analysis
    RCAFrameworkExecutionResult result = this.frameworks.get(framework).run(input);

    // apply formatters
    return applyFormatters(result.getResultsSorted());
  }

  private List<RootCauseEntity> applyFormatters(Iterable<Entity> entities) {
    List<RootCauseEntity> output = new ArrayList<>();
    for(Entity e : entities) {
      output.add(applyFormatters(e));
    }
    return output;
  }

  private RootCauseEntity applyFormatters(Entity e) {
    for(RootCauseEntityFormatter formatter : this.formatters) {
      if(formatter.applies(e)) {
        try {
          return formatter.format(e);
        } catch (Exception ex) {
          LOG.warn("Error applying formatter '{}'. Skipping.", formatter.getClass().getName(), ex);
        }
      }
    }
    throw new IllegalArgumentException(String.format("No formatter for Entity '%s'", e.getUrn()));
  }

  /**
   * Support both multi-entity notations:
   * <br/><b>(1) comma-delimited:</b> {@code "urns=thirdeye:metric:123,thirdeye:metric:124"}
   * <br/><b>(2) multi-param</b> {@code "urns=thirdeye:metric:123&urns=thirdeye:metric:124"}
   *
   * @param urns
   * @return
   */
  private static List<String> parseUrnsParam(List<String> urns) {
    if(urns.size() != 1)
      return urns;
    return Arrays.asList(urns.get(0).split(","));
  }
}
