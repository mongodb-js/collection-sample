var proxyquire = require('proxyquire');
var assert = require('assert');
var _range = require('lodash.range');
var es = require('event-stream');
var mongodb = require('mongodb');
var ReadPreference = require('mongodb-read-preference');
var sample = require('../');
var ReservoirSampler = require('../lib/reservoir-sampler');
var NativeSampler = require('../lib/native-sampler');
var runner = require('mongodb-runner');

var debug = require('debug')('mongodb-collection-sample:test');

var getSampler = function(version, fn) {
  proxyquire('../lib', {
    'get-mongodb-version': function(opts, cb) {
      process.nextTick(function() {
        cb(null, version);
      });
    }
  }).getSampler({}, 'pets', {}, fn);
};

var runnerOpts = {
  topology: 'replicaset'
};

before(function(done) {
  this.timeout(40000);
  debug('launching local replicaset.');
  runner(runnerOpts, done);
});

after(function(done) {
  this.timeout(20000);
  debug('stopping replicaset.');
  runner.stop(runnerOpts, done);
});

describe('mongodb-collection-sample', function() {
  before(function(done) {
    // output the current version for debug purpose
    mongodb.MongoClient.connect('mongodb://localhost:27017/test', function(err, db) {
      assert.ifError(err);
      db.admin().serverInfo(function(err2, info) {
        assert.ifError(err2);
        debug('running tests with MongoDB version %s.', info.version);
        db.close();
        done();
      });
    });
  });

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

  describe('Reservoir Sampler chunk sampling', function() {
    var db;

    before(function(done) {
      this.timeout(10000);
      mongodb.MongoClient.connect('mongodb://localhost:27017/test', function(err, _db) {
        if (err) {
          return done(err);
        }
        db = _db;

        var docs = _range(0, 15000).map(function(i) {
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

    it('should use `_id: -1` as the default sort', function(done) {
      getSampler('3.1.5', function(err, src) {
        assert.ifError(err);
        assert.deepEqual(src.sort, {
          _id: -1
        });
        done();
      });
    });

    it('should have the test.haystack collection with 15000 docs', function(done) {
      db.collection('haystack').count(function(err, res) {
        assert.ifError(err);
        assert.equal(res, 15000);
        done();
      });
    });

    it('should sample 10000 docs in several chunks', function(done) {
      var seen = 0;
      sample(db, 'haystack', {
        size: 10000,
        chunkSize: 1234
      })
        .pipe(es.through(function(doc) {
          seen++;
          this.emit('data', doc);
        }, function() {
          this.emit('end');
          assert.equal(seen, 10000);
          done();
        }));
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

  describe('topology', function() {
    this.timeout(30000);

    var dbPrim;
    var dbSec;
    var options = {
      readPreference: ReadPreference.nearest
    };

    before(function(done) {
      mongodb.MongoClient.connect('mongodb://localhost:27017/test', function(err, _dbPrim) {
        if (err) {
          return done(err);
        }
        dbPrim = _dbPrim;
        var docs = _range(0, 100).map(function(i) {
          return {
            _id: 'needle_' + i,
            is_even: i % 2
          };
        });
        dbPrim.collection('haystack').insert(docs, {w: 3}, function() {
          mongodb.MongoClient.connect('mongodb://localhost:27018/test', function(errInsert, _dbSec) {
            if (errInsert) {
              return done(errInsert);
            }
            dbSec = _dbSec;
            dbSec.collection('haystack', options).count(function(errCount, res) {
              assert.ifError(errCount);
              assert.equal(res, 100);
              done();
            });
          });
        });
      });
    });

    after(function(done) {
      if (!dbPrim) {
        return done();
      }
      dbPrim.dropCollection('haystack', function() {
        dbPrim.close();
        dbSec.close();
        done();
      });
    });

    it('should sample correctly when connected to a secondary node', function(done) {
      var opts = {
        size: 5,
        query: {}
      };
      // Get a stream of sample documents from the collection and make sure
      // 5 documents have been returned.
      var count = 0;
      var stream = sample(dbSec, 'haystack', opts);
      stream.on('error', function(err2) {
        done(err2);
      });
      stream.on('data', function() {
        count++;
      });
      stream.on('end', function() {
        assert.equal(count, opts.size);
        done();
      });
    });
  });
});
