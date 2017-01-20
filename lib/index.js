var es = require('event-stream');
var getKernelVersion = require('get-mongodb-version');
var semver = require('semver');
var debug = require('debug')('mongodb-collection-sample');
var ReservoirSampler = require('./reservoir-sampler');
var NativeSampler = require('./native-sampler');

/**
 * Test the MongoDB kernel version `db` is using and return
 * a `stream.Readable` that will use the native `$sample` aggregation
 * operator if available or fall back to a client-side reservoir sample.
 *
 * @param {mongodb.DB} db
 * @param {String} collectionName to source from.
 * @param {Object} opts
 * @param {Function} done callback
 *
 * @option {Object} query to refine possible samples [default: `{}`].
 * @option {Number} size of the sample to capture [default: `5`].
 */
function getSampler(db, collectionName, opts, done) {
  getKernelVersion({
    db: db
  }, function(err, version) {
    var supported = false;
    if (!err) {
      supported = semver.gte(version, '3.1.6');
    }

    debug('has native $sample?', supported);
    if (!supported) {
      return done(null, new ReservoirSampler(db, collectionName, opts));
    }
    return done(null, new NativeSampler(db, collectionName, opts));
  });
}

/**
 * Take an `_id` and emit the source document.
 *
 * @param {mongodb.Db} db
 * @param {String} collectionName to source from.
 * @param {Object} [opts]
 * @option {Object} query to refine possible samples [default: `{}`].
 * @option {Number} size of the sample to capture [default: `5`].
 * @return {stream.Readable}
 * @api public
 */
module.exports = function(db, collectionName, opts) {
  var readable = es.readable(function() {
    getSampler(db, collectionName, opts, function(err, src) {
      if (err) {
        return readable.emit('error', err);
      }

      src.on('data', readable.emit.bind(readable, 'data'));
      src.on('error', readable.emit.bind(readable, 'error'));
      src.on('end', readable.emit.bind(readable, 'end'));
    });
  });
  return readable;
};

module.exports.getSampler = getSampler;
