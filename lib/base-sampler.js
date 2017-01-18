var _defaults = require('lodash.defaults');
var Readable = require('stream').Readable;
var inherits = require('util').inherits;
var ReadPreference = require('mongodb-read-preference');

function BaseSampler(db, collectionName, opts) {
  this.db = db;
  this.collectionName = collectionName;

  opts = _defaults(opts || {}, {
    filter: {},
    size: 5,
    project: null,
    sort: {
      _id: -1
    },
    maxTimeMS: undefined,
    promoteValues: true
  });

  this.filter = opts.filter || {};
  this.size = opts.size;
  this.project = opts.project;
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
    var options = {
      readPreference: ReadPreference.secondaryPreferred
    };
    return this.db.collection(this.collectionName, options);
  }
});

module.exports = BaseSampler;
