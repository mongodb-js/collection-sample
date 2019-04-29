var BaseSampler = require('./base-sampler');
var inherits = require('util').inherits;
var es = require('event-stream');
var Reservoir = require('reservoir');
var _defaults = require('lodash.defaults');
var _chunk = require('lodash.chunk');

var debug = require('debug')('mongodb-collection-sample:reservoir-sampler');

var RESERVOIR_SAMPLE_LIMIT = 10000;
var RESERVOIR_CHUNK_SIZE = 1000;

/**
 * Does reservoir sampling and fetches the resulting documents from the
 * collection.
 *
 * The query runs with a limit of `RESERVOIR_SAMPLE_LIMIT`. The reservoir
 * then samples `size` _ids from the query result. These _ids get grouped
 * into chunks of at most `RESERVOIR_CHUNK_SIZE`. For each chunk, a cursor
 * is opened to fetch the actual documents. The cursor streams are combined
 * and all the resulting documents are emitted downstream.

 * @param  {mongodb.Collection} collection The collection to sample from.
 * @param  {Number} size How many documents should be returned.
 * @param  {Object} opts
 * @option {Object} query to refine possible samples [default: `{}`].
 * @option {Number} size of the sample to capture [default: `5`].
 * @option {Object} fields to return for each document [default: `null`].
 * @option {Boolean} return document results as raw BSON buffers [default: `false`].
 * @option {Number} chunkSize For chunked $in queries [default: `1000`].
 * @return {stream.Readable}
 * @api private
 */
function reservoirStream(collection, size, opts) {
  opts = _defaults(opts || {}, {
    chunkSize: RESERVOIR_CHUNK_SIZE,
    promoteValues: true
  });
  var reservoir = new Reservoir(size);

  var stream = es.through(
    function write(data) {
      // fill reservoir with ids
      reservoir.pushSome(data);
    },
    function end() {
      // split the reservoir of ids into smaller chunks
      var chunks = _chunk(reservoir, opts.chunkSize);
      // create cursors for chunks
      var cursors = chunks.map(function(ids) {
        var cursor = collection.find({ _id: { $in: ids }}, { promoteValues: opts.promoteValues, raw: opts.raw });
        if (opts.fields) {
          cursor.project(opts.fields);
        }
        if (opts.maxTimeMS) {
          cursor.maxTimeMS(opts.maxTimeMS);
        }
        return cursor.stream();
      });
      // merge all cursors (order arbitrary) and emit docs
      es.merge(cursors).pipe(es.through(function(doc) {
        stream.emit('data', doc);
      })).on('end', function() {
        stream.emit('end');
      });
    }
  );
  return stream;
}

/**
 * A readable stream of sample of documents from a collection via
 * [reservoir sampling](https://en.wikipedia.org/wiki/Reservoir_sampling).
 *
 * @param {mongodb.DB} db
 * @param {String} collectionName to source from.
 * @param {Object} opts
 * @option {Object} query to refine possible samples [default: `{}`].
 * @option {Number} size of the sample to capture [default: `5`].
 * @option {Object} fields to return for each document [default: `null`].
 * @option {Number} chunkSize for chunked $in queries [default: `1000`].
 * @api public
 */
function ReservoirSampler(db, collectionName, opts) {
  this.running = false;
  this.chunkSize = opts ? opts.chunkSize : undefined;
  BaseSampler.call(this, db, collectionName, opts);
}
inherits(ReservoirSampler, BaseSampler);

ReservoirSampler.prototype._read = function() {
  if (this.running) {
    return;
  }
  this.running = true;

  debug('using query `%j`', this.query);
  this.collection.countDocuments(this.query, function(err, count) {
    if (err) {
      return this.emit('error', err);
    }

    debug('sampling %d documents from a collection with %d documents',
      this.size, count);

    this.collection.find(this.query, {
      projection: { _id: 1 },
      sort: this.sort,
      limit: RESERVOIR_SAMPLE_LIMIT
    })
      .stream()
      .pipe(es.map(function(obj, cb) {
        return cb(null, obj._id);
      }))
      .pipe(reservoirStream(this.collection, this.size, {
        chunkSize: this.chunkSize,
        fields: this.fields,
        maxTimeMS: this.maxTimeMS,
        raw: this.raw,
        promoteValues: this.promoteValues
      }))
      .on('error', this.emit.bind(this, 'error'))
      .on('data', this.push.bind(this))
      .on('end', function() {
        this.running = false;
        this.push(null);
      }.bind(this));
  }.bind(this));
};

module.exports = ReservoirSampler;
