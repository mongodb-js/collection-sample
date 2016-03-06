var _defaults = require('lodash.defaults');
var Readable = require('stream').Readable;
var inherits = require('util').inherits;
var ReadPreference = require('mongodb-read-preference');

function BaseSampler(db, collectionName, opts) {
  this.db = db;
  this.collectionName = collectionName;

  opts = _defaults(opts || {}, {
    query: {},
    size: 5,
    fields: null,
    sort: {
      _id: -1
    },
    maxTimeMS: undefined
  });

  this.query = opts.query || {};
  this.size = opts.size;
  this.fields = opts.fields;
  this.sort = opts.sort;
  this.maxTimeMS = opts.maxTimeMS;

  Readable.call(this, {
    objectMode: true
  });
}
inherits(BaseSampler, Readable);

Object.defineProperty(BaseSampler.prototype, 'collection', {
  get: function() {
    var options = {
      readPreference: ReadPreference.nearest
    };
    return this.db.collection(this.collectionName, options);
  }
});

module.exports = BaseSampler;
