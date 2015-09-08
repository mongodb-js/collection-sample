var BaseSampler = require('./base-sampler');
var inherits = require('util').inherits;
var createReservoir = require('reservoir-stream');
var es = require('event-stream');
var _defaults = require('lodash.defaults');
var debug = require('debug')('mongodb-collection-sample:reservoir-sampler');

/**
 * A readable stream of sample of documents from a collection via
 * [reservoir sampling](https://en.wikipedia.org/wiki/Reservoir_sampling).
 *
 * @param {mongodb.DB} db
 * @param {String} collection_name to source from.
 * @param {Object} opts
 * @option {Object} query to refine possible samples [default: `{}`].
 * @option {Number} size of the sample to capture [default: `5`].
 * @option {Object} fields to return for each document [default: `null`].
 * @api public
 */
function ReservoirSampler(db, collection_name, opts) {
  this.running = false;
  BaseSampler.call(this, db, collection_name, opts);
}
inherits(ReservoirSampler, BaseSampler);

ReservoirSampler.prototype._read = function() {
  if (this.running) return;

  this.running = true;

  debug('using query `%j`', this.query);
  this.collection.count(this.query, function(err, count) {
    if (err) return this.emit('error', err);

    debug('sampling %d documents from a collection with %d documents',
      this.size, count);

    this.collection.find(this.query, {
      fields: {
        _id: 1
      },
      sort: this.sort,
      limit: 100000
    })
      .stream()
      .pipe(createReservoir(this.size))
      .pipe(_idToDocument(this.db, this.collection_name, {
        fields: this.fields
      }))
      .on('error', this.emit.bind(this, 'error'))
      .on('data', this.push.bind(this))
      .on('end', function() {
        this.running = false;
        this.push(null);
      }.bind(this));
  }.bind(this));
};

/**
 * Take an `_id` and emit the source document.
 *
 * @param {mongodb.DB} db
 * @param {String} collection_name to source from.
 * @param {Object} opts
 * @option {Object} fields to return for each document [default: `null`].
 * @return {stream.Transform}
 * @api private
 */
function _idToDocument(db, collection_name, opts) {
  opts = _defaults(opts || {}, {
    fields: null
  });

  var collection = db.collection(collection_name);
  return es.map(function(_id, fn) {
    var query = _id;
    if (!_id._id) {
      query = {
        _id: _id
      };
    }

    var options = {
      fields: opts.fields
    };
    debug('findOne `%j`', query);

    collection.findOne(query, options, function(err, doc) {
      if (err) {
        debug('error pulling document: ', err);
        return fn(err);
      }
      debug('pulled document for _id `%j`', doc._id);
      fn(null, doc);
    });
  });
}

module.exports = ReservoirSampler;
