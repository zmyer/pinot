import Controller from '@ember/controller';
import floatToPercent from 'thirdeye-frontend/utils/float-to-percent';
import { computed, set } from '@ember/object';
import moment from 'moment';
import { setUpTimeRangeOptions } from 'thirdeye-frontend/utils/manage-alert-utils';

const TIME_PICKER_INCREMENT = 5; // tells date picker hours field how granularly to display time
const DEFAULT_ACTIVE_DURATION = '1d'; // setting this date range selection as default (Last 24 Hours)
const UI_DATE_FORMAT = 'MMM D, YYYY hh:mm a'; // format for date picker to use (usually varies by route or metric)
const DISPLAY_DATE_FORMAT = 'YYYY-MM-DD HH:mm'; // format used consistently across app to display custom date range
const TIME_RANGE_OPTIONS = ['today', '1d', '1w'];

export default Controller.extend({
  queryParams: ['appName', 'startDate', 'endDate', 'duration'],
  appName: null,
  startDate: null,
  endDate: null,
  duration: null,

  /**
   * Overrides ember-models-table's css classes
   */
  classes: {
    table: 'table table-striped table-bordered table-condensed'
  },

  /**
   * Stats to display in cards
   * @type {Object[]} - array of objects, each of which represents a stats card
   */
  pill: computed(
    'model.{appName,startDate,endDate,duration}',
    function() {
      const appName = this.get('model.appName');
      const startDate = Number(this.get('model.startDate'));
      const endDate = Number(this.get('model.endDate'));
      const duration = this.get('model.duration') || DEFAULT_ACTIVE_DURATION;
      return {
        appName,
        uiDateFormat: UI_DATE_FORMAT,
        activeRangeStart: moment(startDate).format(DISPLAY_DATE_FORMAT),
        activeRangeEnd: moment(endDate).format(DISPLAY_DATE_FORMAT),
        timeRangeOptions: setUpTimeRangeOptions(TIME_RANGE_OPTIONS, duration),
        timePickerIncrement: TIME_PICKER_INCREMENT
      };
    }
  ),

  /**
   * Stats to display in cards
   * @type {Object[]} - array of objects, each of which represents a stats card
   */
  stats: computed(
    'model.anomalyPerformance',
    function() {
      if (!this.get('model.anomalyPerformance')) {
        return {};
      }

      const { totalAlerts, responseRate, precision, recall } = this.get('model.anomalyPerformance').getProperties('totalAlerts', 'responseRate', 'precision', 'recall');
      const totalAlertsDescription = 'Total number of anomalies that occured over a period of time';
      const responseRateDescription = '% of anomalies that are reviewed';
      const precisionDescription = '% of all anomalies detected by the system that are true';
      const recallDescription = '% of all anomalies detected by the system';
      const statsArray = [
        ['Number of anomalies', totalAlertsDescription, totalAlerts, 'digit'],
        ['Response Rate', responseRateDescription, floatToPercent(responseRate), 'percent'],
        ['Precision', precisionDescription, floatToPercent(precision), 'percent'],
        ['Recall', recallDescription, floatToPercent(recall), 'percent']
      ];

      return statsArray;
    }
  ),

  actions: {
    /**
     * Sets the selected application property based on user selection
     * @param {Object} selectedApplication - object that represents selected application
     * @return {undefined}
     */
    selectApplication(selectedApplication) {
      set(this, 'appName', selectedApplication.get('application'));
    },

    /**
     * Sets the new custom date range for anomaly coverage
     * @method onRangeSelection
     * @param {Object} rangeOption - the user-selected time range to load
     */
    onRangeSelection(timeRangeOptions) {
      const {
        start,
        end,
        value: duration
      } = timeRangeOptions;

      const startDate = moment(start).valueOf();
      const endDate = moment(end).valueOf();
      const appName = this.get('appName');
      //Update the time range option selected
      this.set('timeRangeOptions', setUpTimeRangeOptions(TIME_RANGE_OPTIONS, duration));
      this.transitionToRoute({ queryParams: { appName, duration, startDate, endDate }});
    }
  }
});
