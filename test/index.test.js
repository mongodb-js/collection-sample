var proxyquire = require('proxyquire');
var assert = require('assert');
var _range = require('lodash.range');
var es = require('event-stream');
var mongodb = require('mongodb');
var sample = require('../');
var ReservoirSampler = require('../lib/reservoir-sampler');
var NativeSampler = require('../lib/native-sampler');

var getSampler = function(version, fn) {
  proxyquire('../lib', {
    'get-mongodb-version': function(opts, cb) {
      process.nextTick(function() {
        cb(null, version);
      });
    }
  }).getSampler({}, 'pets', {}, fn);
};

describe('mongodb-collection-sample', function() {
  describe('polyfill', function() {
    it('should use reservoir sampling if version is 3.1.5', function(done) {
      getSampler('3.1.5', function(err, src) {
        assert.ifError(err);
        assert(src instanceof ReservoirSampler);
        done();
      });
    });

    it('should use native sampling if version is 3.1.6', function(done) {
      getSampler('3.1.6', function(err, src) {
        assert.ifError(err);
        assert(src instanceof NativeSampler);
        done();
      });
    });

    it('should use native sampling if version is 3.1.7', function(done) {
      getSampler('3.1.7', function(err, src) {
        assert.ifError(err);
        assert(src instanceof NativeSampler);
        done();
      });
    });
  });
  describe('reservoir', function() {
    it('should use `_id: -1` as the default sort', function(done) {
      getSampler('3.1.5', function(err, src) {
        assert.ifError(err);
        assert.deepEqual(src.sort, {
          _id: -1
        });
        done();
      });
    });
  });

  describe('functional', function() {
    var db;

    before(function(done) {
      mongodb.MongoClient.connect('mongodb://localhost:27017/test', function(err, _db) {
        if (err) {
          return done(err);
        }
        db = _db;

        var docs = _range(0, 1000).map(function(i) {
          return {
            _id: 'needle_' + i,
            is_even: i % 2
          };
        });
        db.collection('haystack').insert(docs, done);
      });
    });

    after(function(done) {
      if (!db) {
        return done();
      }
      db.dropCollection('haystack', done);
    });

    it('should should default the sample size to `5`', function(done) {
      var seen = 0;
      sample(db, 'haystack')
        .pipe(es.through(function(doc) {
          seen++;
          this.emit('data', doc);
        }, function() {
          this.emit('end');
          assert.equal(seen, 5);
          done();
        }));
    });

    it('should allow specifying a query', function(done) {
      var docs = [];
      var options = {
        size: 10,
        query: {
          is_even: 1
        }
      };
      sample(db, 'haystack', options)
        .pipe(es.through(function(doc) {
          docs.push(doc);
          this.emit('data', doc);
        }, function() {
          this.emit('end');
          assert.equal(docs.filter(function(d) {
            return d.is_even === 1;
          }).length, options.size);
          done();
        }));
    });

    it('should get a sample of 10 documents', function(done) {
      var seen = 0;
      sample(db, 'haystack')
        .pipe(es.through(function(doc) {
          seen++;
          this.emit('data', doc);
        }, function() {
          this.emit('end');
          assert.equal(seen, 5);
          done();
        }));
    });

    it('should return as many documents as possible if '
      + 'the requested sample size is larger than the '
      + 'collection size', function(done) {
      var seen = 0;
      sample(db, 'haystack', {
        size: 2000
      })
        .pipe(es.through(function(doc) {
          seen++;
          this.emit('data', doc);
        }, function() {
          this.emit('end');
          assert.equal(seen, 1000);
          done();
        }));
    });
  });
});
