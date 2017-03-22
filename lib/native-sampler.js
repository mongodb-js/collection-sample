var BaseSampler = require('./base-sampler');
var ReservoirSampler = require('./reservoir-sampler');
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
 * @option {Array} fields to only return certain fields [default: null]
 * @option {Number} size of the sample to capture [default: `5`].
 * @api public
 */
function NativeSampler(db, collectionName, opts) {
  BaseSampler.call(this, db, collectionName, opts);

  this.running = false;

  // match
  this.pipeline = [];
  if (Object.keys(this.query).length > 0) {
    this.pipeline.push({
      $match: this.query
    });
  }

  // sample size
  this.pipeline.push({
    $sample: {
      size: this.size
    }
  });

  // projection (last operation, only needs to be applied to sampled docs)
  if (this.fields && Object.keys(this.fields).length > 0) {
    this.pipeline.push({
      $project: this.fields
    });
  }
}
inherits(NativeSampler, BaseSampler);

NativeSampler.prototype._read = function() {
  if (this.running) {
    return;
  }

  this.running = true;

  var options = {
    maxTimeMS: this.maxTimeMS,
    allowDiskUse: true,
    promoteValues: this.promoteValues
  };

  this.collection.count(this.query, options, function(err, count) {
    if (err) {
      return this.emit('error', err);
    }

    debug('sampling %d documents from a collection with %d documents',
      this.size, count);

    // if we need more docs than counted, use find to return all docs
    if (count <= this.size) {
      return this.collection.find(this.query, {
        fields: this.fields
      }).on('error', this.emit.bind(this, 'error'))
        .on('data', this.push.bind(this))
        .on('end', this.push.bind(this, null));
    }

    // if we need more than 5% of all docs, use ReservoirSampler to avoid
    // the blocking sort stage (SERVER-22815)
    if (count <= this.size * 20) {
      var reservoirSampler = new ReservoirSampler(this.db, this.collectionName, this.opts);
      return reservoirSampler
      .on('error', this.emit.bind(this, 'error'))
      .on('data', this.push.bind(this))
      .on('end', this.push.bind(this, null));
    }

    // else, use native sampler with random index walk optimization in the Server
    this.collection.aggregate(this.pipeline, options)
      .on('error', this.emit.bind(this, 'error'))
      .on('data', this.push.bind(this))
      .on('end', this.push.bind(this, null));
  }.bind(this));
};

module.exports = NativeSampler;
