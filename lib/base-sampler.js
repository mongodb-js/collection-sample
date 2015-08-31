var _defaults = require('lodash.defaults');
var Readable = require('stream').Readable;
var inherits = require('util').inherits;

function BaseSampler(db, collection_name, opts) {
  this.db = db;
  this.collection_name = collection_name;

  opts = _defaults(opts || {}, {
    query: {},
    size: 5,
    fields: null,
    sort: {
      $natural: -1
    }
  });

  this.query = opts.query;
  this.size = opts.size;
  this.fields = opts.fields;
  this.sort = opts.sort;

  Readable.call(this, {
    objectMode: true
  });
}
inherits(BaseSampler, Readable);

Object.defineProperty(BaseSampler.prototype, 'collection', {
  get: function() {
    return this.db.collection(this.collection_name);
  }
});

module.exports = BaseSampler;
