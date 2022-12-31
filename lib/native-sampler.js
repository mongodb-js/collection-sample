const BaseSampler = require('./base-sampler');
const rawTransform = require('./raw-transform');
const ReservoirSampler = require('./reservoir-sampler');
const inherits = require('util').inherits;
const debug = require('debug')('mongodb-collection-sample:native-sampler');

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
 * @option {Boolean} return document results as raw BSON buffers [default: `false`].
 * @api public
 */
function NativeSampler(db, collectionName, opts) {
  BaseSampler.call(this, db, collectionName, opts);
  this.running = false;
}
inherits(NativeSampler, BaseSampler);

NativeSampler.prototype._read = function() {
  if (this.running) {
    return;
  }

  this.running = true;

  const options = {
    maxTimeMS: this.maxTimeMS,
    allowDiskUse: true,
    promoteValues: this.promoteValues
  };

  this.collection.countDocuments(this.query, options, function(err, count) {
    if (err) {
      return this.emit('error', err);
    }
    debug('sampling %d documents from a collection with %d documents',
      this.size, count);

    // if we need more than 5% of all docs (but not all of them), use
    // ReservoirSampler to avoid the blocking sort stage (SERVER-22815).
    // if need raw output, always do native sampling
    if (count > this.size && count <= this.size * 20) {
      const reservoirSampler = new ReservoirSampler(this.db, this.collectionName, this.opts);
      return reservoirSampler
        .on('error', this.emit.bind(this, 'error'))
        .on('data', this.push.bind(this))
        .on('end', this.push.bind(this, null));
    }
    // else, use native sampler

    // add $match stage if a query was specified
    this.pipeline = [];
    if (Object.keys(this.query).length > 0) {
      this.pipeline.push({
        $match: this.query
      });
    }

    // only add $sample stage if the result set contains more
    // documents than requested
    if (count > this.size) {
      this.pipeline.push({
        $sample: {
          size: this.size
        }
      });
    }

    // add $project stage if projection (fields) was specified
    if (this.fields && Object.keys(this.fields).length > 0) {
      this.pipeline.push({
        $project: this.fields
      });
    }

    options.raw = this.raw;
    options.cursor = this.cursor;
    options.batchSize = this.size;

    const cursor = this.collection.aggregate(this.pipeline, options);

    cursor
      .stream()
      .pipe(rawTransform(this.raw))
      .on('error', this.emit.bind(this, 'error'))
      .on('data', this.push.bind(this))
      .on('end', this.push.bind(this, null));
  }.bind(this));
};

module.exports = NativeSampler;
