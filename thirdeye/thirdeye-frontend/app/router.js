import EmberRouter from '@ember/routing/router';
import config from './config/environment';

const Router = EmberRouter.extend({
  location: config.locationType,
  rootURL: config.rootURL
});

Router.map(function() {
  this.route('login');
  this.route('logout');
  //this.route('error'); disable for now

  this.route('manage', function() {
    this.route('alert', { path: 'alert/:alert_id' }, function() {
      this.route('explore');
      this.route('tune');
      this.route('edit');
    });
    this.route('alerts', function() {
      this.route('performance');
    });
  });

  this.route('rca', function() {
    this.route('details', { path: '/:metric_id' }, function () {
      this.route('metrics');
      this.route('events');
      this.route('dimensions', function() {
        this.route('heatmap', {path: '/'});
      });
    });
  });

  this.route('self-serve', function() {
    this.route('create-alert');
    this.route('import-metric');
  });

  this.route('screenshot', { path: 'screenshot/:anomaly_id' });
  this.route('rootcause');
  this.route('home');
});

export default Router;
