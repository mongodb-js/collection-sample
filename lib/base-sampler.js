var _defaults = require('lodash.defaults');
var Readable = require('stream').Readable;
var inherits = require('util').inherits;

function BaseSampler(db, collectionName, opts) {
  this.db = db;
  this.collectionName = collectionName;
  this.opts = opts;

  opts = _defaults(opts || {}, {
    query: {},
    size: 5,
    fields: null,
    raw: false,
    sort: {
      _id: -1
    },
    maxTimeMS: undefined,
    promoteValues: true
  });

  this.query = opts.query || {};
  this.size = opts.size;
  this.raw = opts.raw;
  this.fields = opts.fields;
  this.sort = opts.sort;
  this.maxTimeMS = opts.maxTimeMS;
  this.promoteValues = opts.promoteValues;

  Readable.call(this, {
    objectMode: true
  });
}
inherits(BaseSampler, Readable);

Object.defineProperty(BaseSampler.prototype, 'collection', {
  get: function() {
    return this.db.collection(this.collectionName, {});
  }
});

module.exports = BaseSampler;
