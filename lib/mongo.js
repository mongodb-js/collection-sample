/* global DBCollection: false, assert: false */

// Fix broken type checks in src/mongo/shell/types.js
// @see https://jira.mongodb.org/browse/SERVER-14220
var isObject = function(value) {
  return Object.prototype.toString.call(value) === '[object Object]';
};

var isNull = function(value) {
  return Object.prototype.toString.call(value) === '[object Null]';
};

function _reduceFields(fields, doc) {

  if (isNull(fields)) {
    return doc;
  }

  return Object.keys(fields).reduce(function _fieldReducer(res, path) {
    set(path, get(path, doc), res);
    return res;
  }, {});
}

function parsePath(path) {
  if (path.indexOf('[') >= 0) {
    path = path.
    replace(/\[/g, '.').
    replace(/]/g, '');
  }
  return path.split('.');
}

function parseKey(key, val) {
  // detect negative index notation
  if (key[0] === '-' && Array.isArray(val) && /^-\d+$/.test(key)) {
    return val.length + parseInt(key, 10);
  }
  return key;
}

function get(path, obj) {
  var key, i;
  var keys = parsePath(path);

  for (i = 0; i < keys.length; i++) {
    key = parseKey(keys[i], obj);
    if (obj && typeof obj === 'object' && key in obj) {
      if (i === (keys.length - 1)) {
        return obj[key];
      } else {
        obj = obj[key];
      }
    } else {
      return undefined;
    }
  }
  return obj;
}

function set(path, val, obj) {
  var key, k, i;
  var keys = parsePath(path);

  for (i = 0; i < keys.length; i++) {
    key = keys[i];
    if (i === (keys.length - 1)) {
      if (isObject(val) && isObject(obj[key])) {
        for (k in val) {
          if (val.hasOwnProperty(k)) {
            obj[key][k] = val[k];
          }
        }

      } else if (Array.isArray(obj[key]) && Array.isArray(val)) {
        for (var j = 0; j < val.length; j++) {
          obj[keys[i]].push(val[j]);
        }
      } else {
        obj[key] = val;
      }
    } else if (
      // force the value to be an object
      !obj.hasOwnProperty(key) ||
      (!isObject(obj[key]) && !Array.isArray(obj[key]))
    ) {
      // initialize as array if next key is numeric
      if (/^\d+$/.test(keys[i + 1])) {
        obj[key] = [];
      } else {
        obj[key] = {};
      }
    }
    obj = obj[key];
  }
  return obj;
}

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
    _index: 0,
    isNull: isNull,
    isObject: isObject,
    _reduceFields: _reduceFields,
    set: set,
    get: get,
    parseKey: parseKey,
    parsePath: parsePath
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
        emit(_index, _reduceFields(fields, this));
      } else {
        // With decreasing probability, replace sampled
        // documents with new documents from the collection.
        var _randomIndex = Math.floor(Math.random() * _index);
        if (_randomIndex < size) {
          emit(_randomIndex, _reduceFields(fields, this));
        }
      }
      _index++;
    },
    reduce: function(key, value) {
      return value[value.length - 1];
    },
    query: query,
    out: {
      inline: 1
    },
    scope: scope,
  });

  if (!res.ok) {
    return print("Error: " + res.errmsg);
  }

  return res.results.map(function(d) {
    return d.value;
  });
};
