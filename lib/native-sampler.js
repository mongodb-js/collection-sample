var BaseSampler = require('./base-sampler');
var inherits = require('util').inherits;
var debug = require('debug')('mongodb-collection-sample:native-sampler');

/**
 * A readable stream of sample of documents from a collection using the
 * `$sample` aggregation operator.
 *
 * @param {mongodb.DB} db
 * @param {String} collectionName to source from.
 * @param {Object} opts
 * @option {Object} query to refine possible samples [default: `{}`].
 * @option {Number} size of the sample to capture [default: `5`].
 * @api public
 */
function NativeSampler(db, collectionName, opts) {
  BaseSampler.call(this, db, collectionName, opts);

  this.running = false;

  this.pipeline = [];
  if (Object.keys(this.query).length > 0) {
    this.pipeline.push({
      $match: this.query
    });
  }

  this.pipeline.push({
    $sample: {
      size: this.size
    }
  });
}
inherits(NativeSampler, BaseSampler);

NativeSampler.prototype._read = function() {
  if (this.running) {
    return;
  }

  this.running = true;

  var options = {
    maxTimeMS: this.maxTimeMS
  };

  this.collection.count(this.query, options, function(err, count) {
    if (err) {
      return this.emit('error', err);
    }

    debug('sampling %d documents from a collection with %d documents',
      this.size, count);

    this.collection.aggregate(this.pipeline, options)
      .on('error', this.emit.bind(this, 'error'))
      .on('data', this.push.bind(this))
      .on('end', this.push.bind(this, null));
  }.bind(this));
};

module.exports = NativeSampler;
