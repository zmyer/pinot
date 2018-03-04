import Controller from '@ember/controller';
import floatToPercent from 'thirdeye-frontend/utils/float-to-percent';
import { computed, get } from '@ember/object';

export default Controller.extend({

  /**
   * Stats to display in cards
   * @type {Object[]} - array of objects, each of which represents a stats card
   */
  stats: computed(
    'model.anomalyPerformance',
    function() {
      const { totalAlerts, responseRate, precision, recall } = get(this, 'model.anomalyPerformance');
      const totalAlertsDescription = 'Total number of anomalies that occured over a period of time';
      const responseRateDescription = '% of anomalies that are reviewed';
      const precisionDescription = '% of all anomalies detected by the system that are true';
      const recallDescription = '% of all anomalies detected by the system';
      const statsArray = [
        ['Number of anomalies', totalAlertsDescription, totalAlerts],
        ['Response Rate', responseRateDescription, floatToPercent(responseRate)],
        ['Precision', precisionDescription, floatToPercent(precision)],
        ['Recall', recallDescription, floatToPercent(recall)]
      ];

      return statsArray;
    }
  )
});
