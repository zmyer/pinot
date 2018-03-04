import Route from '@ember/routing/route';
import applicationAnomalies from 'thirdeye-frontend/mirage/fixtures/applicationAnomalies';
import anomalyPerformance from 'thirdeye-frontend/mirage/fixtures/anomalyPerformance';
import { humanizeFloat } from 'thirdeye-frontend/utils/utils';
import columns from 'thirdeye-frontend/shared/anomaliesTableColumns';
import RSVP from 'rsvp';

export default Route.extend({

  /**
   * Returns a two-dimensional array, which maps anomalies by metric and functionName (aka alert)
   * @return {Object}
   * @example
   * {
   *  "Metric 1": {
   *    "Alert 1": [ {anomalyObject}, {anomalyObject} ],
   *    "Alert 11": [ {anomalyObject}, {anomalyObject} ]
   *  },
   *  "Metric 2": {
   *    "Alert 21": [ {anomalyObject}, {anomalyObject} ],
   *    "Alert 22": [ {anomalyObject}, {anomalyObject} ]
   *   }
   * }
   */
  model() {
    let anomalyMapping = {};

    applicationAnomalies.forEach(anomaly => {
      const { metric, functionName, current, baseline } = anomaly;

      if (!anomalyMapping[metric]) {
        anomalyMapping[metric] = {};
      }

      if (!anomalyMapping[metric][functionName]) {
        anomalyMapping[metric][functionName] = [];
      }

      // Group anomalies by metric and function name
      anomalyMapping[metric][functionName].push(anomaly);

      // Format current and baseline numbers, so numbers in the millions+ don't overflow
      anomaly.current = humanizeFloat(anomaly.current);
      anomaly.baseline = humanizeFloat(anomaly.baseline);
      anomaly.change = (((current - baseline) / baseline) * 100).toFixed(2);
    });

    return RSVP.hash({
      anomalyMapping,
      anomalyPerformance
    });
  },

  /**
   * Retrieves metrics to index anomalies
   * @return {String[]} - array of strings, each of which is a metric
   */
  getMetrics() {
    let metricSet = new Set();

    applicationAnomalies.forEach(anomaly => {
      metricSet.add(anomaly.metric);
    });

    return [...metricSet];
  },

  /**
   * Retrieves alerts to index anomalies
   * @return {String[]} - array of strings, each of which is a alerts
   */
  getAlerts() {
    let alertSet = new Set();
    applicationAnomalies.forEach(anomaly => {
      alertSet.add(anomaly.functionName);
    });

    return [...alertSet];
  },

  /**
   * Sets the table column, metricList, and alertList
   * @return {undefined}
   */
  setupController(controller) {
    this._super(...arguments);

    controller.setProperties({
      columns,
      metricList: this.getMetrics(),
      alertList: this.getAlerts()
    });
  }
});
