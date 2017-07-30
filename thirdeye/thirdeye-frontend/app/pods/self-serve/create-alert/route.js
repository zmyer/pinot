/**
 * Handles the 'create alert' route nested in the 'manage' route.
 * @module self-serve/create/route
 * @exports alert create model
 */
import Ember from 'ember';
import fetch from 'fetch';
import RSVP from 'rsvp';

export default Ember.Route.extend({

  actions: {
    /**
    * Refresh route's model.
    * @method refreshModel
    * @return {undefined}
    */
    refreshModel() {
      this.refresh();
    }
  },

  /**
   * Model hook for the create alert route.
   * @method model
   * @return {Object}
   */
  model() {
    return RSVP.hash({
      // Fetch all alert group configurations
      allConfigGroups: fetch('/thirdeye/entity/ALERT_CONFIG').then(res => res.json()),
      allAppNames: fetch('/thirdeye/entity/APPLICATION').then(res => res.json())
    })
  }
});
