/* global DBCollection: false, assert: false */

// Fix broken type checks in src/mongo/shell/types.js
// @see https://jira.mongodb.org/browse/SERVER-14220
var isObject = function(value) {
  return Object.prototype.toString.call(value) === '[object Object]';
};

var isNull = function(value) {
  return Object.prototype.toString.call(value) === '[object Null]';
};

/**
 * Use [reservoir sampling](http://en.wikipedia.org/wiki/Reservoir_sampling)
 * via map/reduce to collect a representative sample of documents
 * from a collection.
 *
 * Usage:
 *
 * ```javascript
 * // Returns up to 5 documents from the user collection.
 * db.users.sample()
 *
 * // Returns up to 4 documents from the user collection.
 * db.users.sample(4)
 *
 * // Returns up to 3 documents from the user collection where username
 * // starts with L.
 * db.users.sample(3), {username: /^L/})
 //
 * // Returns up to 2 documents from the user collection where username
 * // starts with L and only the username field.
 * db.users.sample(2, {username: /^L/}, {username: 1})
 * ```
 *
 * @param {Number} [size] of the sample to take [default: `5`].
 * @param {Object} [query] to refine the scope of the sample [default: `{}`].
 * @param {Object|null} [fields] to return from the sampled documents [default: `null`].
 * @returns {Array}
 */
DBCollection.prototype.sample = function(size, query, fields) {
  // Scope that will be injected into map and reduce functions.
  var scope = {
    size: parseInt((size || 5), 10),
    query: query || {},
    fields: fields || null,
    _index: 0
  };

  assert(!isNaN(scope.size), 'size must be a number');
  assert(isObject(scope), 'query must be an object');
  assert(isObject(scope.query) || isNull(scope.query), 'fields must be an object or null');


  var res = this._db.runCommand({
    mapReduce: this._shortName,
    map: function() {
      /* global size: false, _index: false, emit: false */
      // Fill the initial sample.
      if (_index < size) {
        emit(_index, this);
      } else {
        // With decreasing probability, replace sampled
        // documents with new documents from the collection.
        var _randomIndex = Math.floor(Math.random() * _index);
        if (_randomIndex < size) {
          emit(_randomIndex, this);
        }
      }
      _index++;
    },
    reduce: function(key, value) {
      return value[value.length - 1];
    },
    out: {
      inline: 1
    },
    scope: scope,
  });

  return res.results.map(function(d) {
    return d.value;
  });
};
