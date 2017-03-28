/* eslint no-unused-expressions: 0 */

var proxyquire = require('proxyquire');
var expect = require('chai').expect;
var _range = require('lodash.range');
var es = require('event-stream');
var mongodb = require('mongodb');
var ReadPreference = require('mongodb-read-preference');
var sample = require('../');
var ReservoirSampler = require('../lib/reservoir-sampler');
var NativeSampler = require('../lib/native-sampler');
var runner = require('mongodb-runner');
var bson = require('bson');
var semver = require('semver');

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
  topology: 'replicaset',
  port: 31017
};

var versionSupportsSample;

var skipIfSampleUnsupported = function() {
  if (!versionSupportsSample) {
    this.skip();
  }
};

before(function(done) {
  this.timeout(100000);
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
    mongodb.MongoClient.connect('mongodb://localhost:31017/test', function(err, db) {
      expect(err).to.not.exist;
      db.admin().serverInfo(function(err2, info) {
        expect(err2).to.not.exist;
        debug('running tests with MongoDB version %s.', info.version);
        versionSupportsSample = semver.gte(info.version, '3.1.6');
        db.close();
        done();
      });
    });
  });

  describe('polyfill', function() {
    it('should use reservoir sampling if version is 3.1.5', function(done) {
      getSampler('3.1.5', function(err, src) {
        expect(err).to.not.exist;
        expect(src).to.be.an.instanceOf(ReservoirSampler);
        done();
      });
    });

    it('should use native sampling if version is 3.1.6', function(done) {
      getSampler('3.1.6', function(err, src) {
        expect(err).to.not.exist;
        expect(src).to.be.an.instanceOf(NativeSampler);
        done();
      });
    });

    it('should use native sampling if version is 3.1.7', function(done) {
      getSampler('3.1.7', function(err, src) {
        expect(err).to.not.exist;
        expect(src).to.be.an.instanceOf(NativeSampler);
        done();
      });
    });
  });

  describe('Native Sampler pipelines', function() {
    this.timeout(30000);
    var db;

    before(function(done) {
      mongodb.MongoClient.connect('mongodb://localhost:31017/test', function(err, _db) {
        if (err) {
          return done(err);
        }
        db = _db;
        var docs = _range(0, 1000).map(function(i) {
          return {
            _id: 'needle_' + i,
            is_even: i % 2,
            long: bson.Long.fromString('1234567890'),
            double: 0.23456,
            int: 1234
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

    context('when requesting 3% of all documents', function() {
      before(skipIfSampleUnsupported);
      var opts = {
        size: 30
      };
      it('has a $sample in the pipeline', function(done) {
        var sampler = new NativeSampler(db, 'haystack', opts);
        sampler
          .on('data', function() {})
          .on('end', function() {
            expect(sampler.pipeline).to.have.lengthOf(1);
            expect(sampler.pipeline[0]).to.have.all.keys('$sample');
            expect(sampler.pipeline[0].$sample).to.be.deep.equal({size: 30});
            done();
          });
      });
    });
    context('when requesting 30% of all documents', function() {
      var opts = {
        size: 300
      };
      it('falls back to reservoir sampling', function(done) {
        var sampler = new NativeSampler(db, 'haystack', opts);
        sampler
          .on('data', function() {})
          .on('end', function() {
            expect(sampler.pipeline).to.not.exist;
            done();
          });
      });
    });
    context('when requesting 300% of all documents', function() {
      var opts = {
        size: 3000
      };
      it('does not contain a $sample in the pipeline', function(done) {
        var sampler = new NativeSampler(db, 'haystack', opts);
        sampler
          .on('data', function() {})
          .on('end', function() {
            expect(sampler.pipeline).to.be.an('array');
            expect(sampler.pipeline).to.have.lengthOf(0);
            done();
          });
      });
    });
    context('when using fields', function() {
      before(skipIfSampleUnsupported);
      var opts = {
        size: 30,
        fields: {'is_even': 1, 'double': 1}
      };
      it('has a $project stage at the end of the pipeline', function(done) {
        var sampler = new NativeSampler(db, 'haystack', opts);
        sampler
          .on('data', function() {})
          .on('end', function() {
            var lastStage = sampler.pipeline[sampler.pipeline.length - 1];
            expect(lastStage).to.have.all.keys('$project');
            expect(lastStage.$project).to.be.deep.equal({is_even: 1, double: 1});
            done();
          });
      });
    });
    context('when using query', function() {
      before(skipIfSampleUnsupported);
      var opts = {
        size: 10,
        query: {is_even: 1}
      };
      it('has a $match stage at the beginning of the pipeline', function(done) {
        var sampler = new NativeSampler(db, 'haystack', opts);
        sampler
          .on('data', function() {})
          .on('end', function() {
            var firstStage = sampler.pipeline[0];
            expect(firstStage).to.have.all.keys('$match');
            expect(firstStage.$match).to.be.deep.equal({is_even: 1});
            done();
          });
      });
    });
  });


  describe('promoteValues', function() {
    var db;

    before(function(done) {
      this.timeout(30000);
      mongodb.MongoClient.connect('mongodb://localhost:31017/test', function(err, _db) {
        if (err) {
          return done(err);
        }
        db = _db;

        var docs = _range(0, 150).map(function(i) {
          return {
            _id: 'needle_' + i,
            is_even: i % 2,
            long: bson.Long.fromString('1234567890'),
            double: 0.23456,
            int: 1234
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

    it('should have the test.haystack collection with 150 docs', function(done) {
      db.collection('haystack').count(function(err, res) {
        expect(err).to.not.exist;
        expect(res).to.be.equal(150);
        done();
      });
    });

    it('should only return the fields requested', function(done) {
      sample(db, 'haystack', {
        size: 10,
        fields: {'is_even': 1, 'double': 1}
      })
        .pipe(es.through(function(doc) {
          expect(doc.is_even).to.exist;
          expect(doc.double).to.exist;
          expect(doc.int).to.be.undefined;
          expect(doc.long).to.be.undefined;
        }, function() {
          this.emit('end');
          done();
        }));
    });

    it('should promote numeric values by default', function(done) {
      sample(db, 'haystack', {
        size: 1,
        chunkSize: 1234
      })
        .pipe(es.through(function(doc) {
          expect(doc.int).to.be.a('number');
          expect(doc.long).to.be.a('number');
          expect(doc.double).to.be.a('number');
          this.emit('data', doc);
        }, function() {
          this.emit('end');
          done();
        }));
    });

    context('when promoteValues is false', function() {
      it('should not promote numeric values', function(done) {
        sample(db, 'haystack', {
          size: 1,
          chunkSize: 1234,
          promoteValues: false
        })
          .pipe(es.through(function(doc) {
            expect(doc.int).to.be.an('object');
            expect(doc.int._bsontype).to.be.equal('Int32');
            expect(doc.long).to.be.an('object');
            expect(doc.long._bsontype).to.be.equal('Long');
            expect(doc.double).to.be.an('object');
            expect(doc.double._bsontype).to.be.equal('Double');
            this.emit('data', doc);
          }, function() {
            this.emit('end');
            done();
          }));
      });
      it('should not promote numeric values when asking for the full collection', function(done) {
        sample(db, 'haystack', {
          size: 999,  // this is more than #docs, which disables $sample
          chunkSize: 1234,
          promoteValues: false
        })
          .pipe(es.through(function(doc) {
            expect(doc.int).to.be.an('object');
            expect(doc.int._bsontype).to.be.equal('Int32');
            expect(doc.long).to.be.an('object');
            expect(doc.long._bsontype).to.be.equal('Long');
            expect(doc.double).to.be.an('object');
            expect(doc.double._bsontype).to.be.equal('Double');
            this.emit('data', doc);
          }, function() {
            this.emit('end');
            done();
          }));
      });
    });
  });

  describe('Reservoir Sampler chunk sampling', function() {
    var db;

    before(function(done) {
      this.timeout(30000);
      mongodb.MongoClient.connect('mongodb://localhost:31017/test', function(err, _db) {
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
        expect(err).to.not.exist;
        expect(src.sort).to.be.deep.equal({
          _id: -1
        });
        done();
      });
    });

    it('should have the test.haystack collection with 15000 docs', function(done) {
      db.collection('haystack').count(function(err, res) {
        expect(err).to.not.exist;
        expect(res).to.be.equal(15000);
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
          expect(seen).to.be.equal(10000);
          done();
        }));
    });
  });

  describe('functional', function() {
    var db;

    before(function(done) {
      mongodb.MongoClient.connect('mongodb://localhost:31017/test', function(err, _db) {
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
          expect(seen).to.be.equal(5);
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
          expect(docs.filter(function(d) {
            return d.is_even === 1;
          }).length).to.be.equal(options.size);
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
          expect(seen).to.be.equal(5);
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
          expect(seen).to.be.equal(1000);
          done();
        }));
    });
  });

  describe('topology', function() {
    this.timeout(30000);

    var dbPrim;
    var dbSec;
    var options = {
      readPreference: ReadPreference.secondaryPreferred
    };

    before(function(done) {
      mongodb.MongoClient.connect('mongodb://localhost:31017/test', function(err, _dbPrim) {
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
          mongodb.MongoClient.connect('mongodb://localhost:31017/test', function(errInsert, _dbSec) {
            if (errInsert) {
              return done(errInsert);
            }
            dbSec = _dbSec;
            dbSec.collection('haystack', options).count(function(errCount, res) {
              expect(errCount).to.not.exist;
              expect(res).to.be.equal(100);
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
        expect(count).to.be.equal(opts.size);
        done();
      });
    });
  });
});
