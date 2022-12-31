/* eslint no-unused-expressions: 0 */
const expect = require('chai').expect;
const _range = require('lodash.range');
const es = require('event-stream');
const { MongoClient, ReadPreference } = require('mongodb');
const sample = require('../');
const NativeSampler = require('../lib/native-sampler');
const bson = require('bson');
const semver = require('semver');

const debug = require('debug')('mongodb-collection-sample:test');

let versionSupportsSample;

const skipIfSampleUnsupported = function() {
  if (!versionSupportsSample) {
    this.skip();
  }
};

describe('mongodb-collection-sample', function() {
  before(function(done) {
    const client = new MongoClient('mongodb://localhost:27018/test');

    // Output the current version for debug purpose.
    client.connect(function(
      err,
      connectedClient
    ) {
      expect(err).to.not.exist;
      connectedClient
        .db('test')
        .admin()
        .serverInfo(function(err2, info) {
          expect(err2).to.not.exist;
          debug('running tests with MongoDB version %s.', info.version);
          versionSupportsSample = semver.gte(info.version, '3.1.6');
          client.close();
          done();
        });
    });
  });

  describe('Native Sampler pipelines', function() {
    this.timeout(30000);
    let db;

    before(function(done) {
      const client = new MongoClient('mongodb://localhost:27018/test');

      client.connect(function(
        err,
        connectedClient
      ) {
        if (err) {
          return done(err);
        }
        db = connectedClient.db('test');
        const docs = _range(0, 1000).map(function(i) {
          return {
            _id: 'needle_' + i,
            is_even: i % 2,
            long: bson.Long.fromString('1234567890'),
            double: 0.23456,
            int: 1234
          };
        });
        db.collection('haystack').insertMany(docs, done);
      } );
    });

    after(function(done) {
      if (!db) {
        return done();
      }
      db.dropCollection('haystack', done);
    });

    context('when requesting 3% of all documents', function() {
      before(skipIfSampleUnsupported);
      const opts = {
        size: 30
      };
      it('has a $sample in the pipeline', function(done) {
        const sampler = new NativeSampler(db, 'haystack', opts);
        sampler
          .on('data', function() {})
          .on('end', function() {
            expect(sampler.pipeline).to.have.lengthOf(1);
            expect(sampler.pipeline[0]).to.have.all.keys('$sample');
            expect(sampler.pipeline[0].$sample).to.be.deep.equal({ size: 30 });
            done();
          });
      });
    });
    context('when requesting 30% of all documents', function() {
      const opts = {
        size: 300
      };
      it('falls back to reservoir sampling', function(done) {
        const sampler = new NativeSampler(db, 'haystack', opts);
        sampler
          .on('data', function() {})
          .on('end', function() {
            expect(sampler.pipeline).to.not.exist;
            done();
          });
      });
    });
    context('when requesting 300% of all documents', function() {
      const opts = {
        size: 3000
      };
      it('does not contain a $sample in the pipeline', function(done) {
        const sampler = new NativeSampler(db, 'haystack', opts);
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
      const opts = {
        size: 30,
        fields: { is_even: 1, double: 1 }
      };
      it('has a $project stage at the end of the pipeline', function(done) {
        const sampler = new NativeSampler(db, 'haystack', opts);
        sampler
          .on('data', function() {})
          .on('end', function() {
            const lastStage = sampler.pipeline[sampler.pipeline.length - 1];
            expect(lastStage).to.have.all.keys('$project');
            expect(lastStage.$project).to.be.deep.equal({
              is_even: 1,
              double: 1
            });
            done();
          });
      });
    });
    context('when using query', function() {
      before(skipIfSampleUnsupported);
      const opts = {
        size: 10,
        query: { is_even: 1 }
      };
      it('has a $match stage at the beginning of the pipeline', function(done) {
        const sampler = new NativeSampler(db, 'haystack', opts);
        sampler
          .on('data', function() {})
          .on('end', function() {
            const firstStage = sampler.pipeline[0];
            expect(firstStage).to.have.all.keys('$match');
            expect(firstStage.$match).to.be.deep.equal({ is_even: 1 });
            done();
          });
      });
    });
  });

  describe('raw buffer', function() {
    let db;

    before(function(done) {
      this.timeout(30000);
      const client = new MongoClient('mongodb://localhost:27018/test');

      client.connect(function(
        err,
        connectedClient
      ) {
        if (err) {
          return done(err);
        }
        db = connectedClient.db('test');

        const docs = _range(0, 150).map(function(i) {
          return {
            _id: 'needle_' + i,
            is_even: i % 2,
            long: bson.Long.fromString('1234567890'),
            double: 0.23456,
            int: 1234
          };
        });
        db.collection('haystack').insertMany(docs, done);
      });
    });

    after(function(done) {
      if (!db) {
        return done();
      }
      db.dropCollection('haystack', done);
    });

    it('should return raw bson buffer when requested', function(done) {
      sample(db, 'haystack', { size: 2, raw: true }).pipe(
        es.through(
          function(doc) {
            expect(Buffer.isBuffer(doc)).to.be.true;
          },
          function() {
            this.emit('end');
            done();
          }
        )
      );
    });
  });

  // This test creates 2mil documents and samples 1mil of those to make sure
  // buffer doesn't overflow. Only creating 2mil documents, since any more
  // causes v8 to run out memory in heap.
  // FYI: takes a while, so will only run with `test=BIG_SAMPLE npm run test`
  if (process.env.test === 'BIG_SAMPLE') {
    describe('raw buffer over a large set of documents', function() {
      let db;

      before(function(done) {
        this.timeout(3000000);
        const client = new MongoClient('mongodb://localhost:27018/test');

        client.connect(function(
          err,
          connectedClient
        ) {
          if (err) {
            return done(err);
          }
          db = connectedClient.db('test');

          const docs = _range(2000000).map(function(i) {
            return {
              _id: 'needle_' + i,
              is_even: i % 2,
              long: bson.Long.fromString('1234567890'),
              double: 0.23456,
              int: 1234
            };
          });
          db.collection('haystack').insertMany(docs, done);
        });
      });

      after(function(done) {
        if (!db) {
          return done();
        }
        db.dropCollection('haystack', done);
      });

      it('buffer does not overflow', function(done) {
        sample(db, 'haystack', { size: 100000, raw: true }).pipe(
          es.through(
            function(doc) {
              expect(Buffer.isBuffer(doc)).to.be.true;
            },
            function() {
              this.emit('end');
              done();
            }
          )
        );
      });
    });
  }

  describe('promoteValues', function() {
    let db;

    before(function(done) {
      this.timeout(30000);
      const client = new MongoClient('mongodb://localhost:27018/test');

      client.connect(function(
        err,
        connectedClient
      ) {
        if (err) {
          return done(err);
        }
        db = connectedClient.db('test');

        const docs = _range(0, 150).map(function(i) {
          return {
            _id: 'needle_' + i,
            is_even: i % 2,
            long: bson.Long.fromString('1234567890'),
            double: 0.23456,
            int: 1234
          };
        });
        db.collection('haystack').insertMany(docs, done);
      });
    });

    after(function(done) {
      if (!db) {
        return done();
      }
      db.dropCollection('haystack', function() {
        done();
      });
    });

    it('should have the test.haystack collection with 150 docs', function(done) {
      db.collection('haystack').countDocuments(function(err, res) {
        expect(err).to.not.exist;
        expect(res).to.be.equal(150);
        done();
      });
    });

    it('should only return the fields requested', function(done) {
      sample(db, 'haystack', {
        size: 10,
        fields: { is_even: 1, double: 1 }
      }).pipe(
        es.through(
          function(doc) {
            expect(doc.is_even).to.exist;
            expect(doc.double).to.exist;
            expect(doc.int).to.be.undefined;
            expect(doc.long).to.be.undefined;
          },
          function() {
            this.emit('end');
            done();
          }
        )
      );
    });

    it('should promote numeric values by default', function(done) {
      sample(db, 'haystack', {
        size: 1,
        chunkSize: 1234
      }).pipe(
        es.through(
          function(doc) {
            expect(doc.int).to.be.a('number');
            expect(doc.long).to.be.a('number');
            expect(doc.double).to.be.a('number');
            this.emit('data', doc);
          },
          function() {
            this.emit('end');
            done();
          }
        )
      );
    });

    context('when promoteValues is false', function() {
      it('should not promote numeric values', function(done) {
        sample(db, 'haystack', {
          size: 1,
          chunkSize: 1234,
          promoteValues: false
        }).pipe(
          es.through(
            function(doc) {
              expect(doc.int).to.be.an('object');
              expect(doc.int._bsontype).to.be.equal('Int32');
              expect(doc.long).to.be.an('object');
              expect(doc.long._bsontype).to.be.equal('Long');
              expect(doc.double).to.be.an('object');
              expect(doc.double._bsontype).to.be.equal('Double');
              this.emit('data', doc);
            },
            function() {
              this.emit('end');
              done();
            }
          )
        );
      });
      it('should not promote numeric values when asking for the full collection', function(done) {
        sample(db, 'haystack', {
          size: 999, // this is more than #docs, which disables $sample
          chunkSize: 1234,
          promoteValues: false
        }).pipe(
          es.through(
            function(doc) {
              expect(doc.int).to.be.an('object');
              expect(doc.int._bsontype).to.be.equal('Int32');
              expect(doc.long).to.be.an('object');
              expect(doc.long._bsontype).to.be.equal('Long');
              expect(doc.double).to.be.an('object');
              expect(doc.double._bsontype).to.be.equal('Double');
              this.emit('data', doc);
            },
            function() {
              this.emit('end');
              done();
            }
          )
        );
      });
    });
  });

  describe('Reservoir Sampler chunk sampling', function() {
    let db;

    before(function(done) {
      this.timeout(30000);
      const client = new MongoClient('mongodb://localhost:27018/test');

      client.connect(function(
        err,
        connectedClient
      ) {
        if (err) {
          return done(err);
        }
        db = connectedClient.db('test');

        const docs = _range(0, 15000).map(function(i) {
          return {
            _id: 'needle_' + i,
            is_even: i % 2
          };
        });
        db.collection('haystack').insertMany(docs, done);
      });
    });

    after(function(done) {
      if (!db) {
        return done();
      }
      db.dropCollection('haystack', function() {
        done();
      });
    });

    it('should have the test.haystack collection with 15000 docs', function(done) {
      db.collection('haystack').countDocuments(function(err, res) {
        expect(err).to.not.exist;
        expect(res).to.be.equal(15000);
        done();
      });
    });

    it('should sample 10000 docs in several chunks', function(done) {
      let seen = 0;
      sample(db, 'haystack', {
        size: 10000,
        chunkSize: 1234
      }).pipe(
        es.through(
          function(doc) {
            seen++;
            this.emit('data', doc);
          },
          function() {
            this.emit('end');
            expect(seen).to.be.equal(10000);
            done();
          }
        )
      );
    });
  });

  describe('functional', function() {
    let db;

    before(function(done) {
      const client = new MongoClient('mongodb://localhost:27018/test');

      client.connect(function(
        err,
        connectedClient
      ) {
        if (err) {
          return done(err);
        }
        db = connectedClient.db('test');

        const docs = _range(0, 1000).map(function(i) {
          return {
            _id: 'needle_' + i,
            is_even: i % 2
          };
        });
        db.collection('haystack').insertMany(docs, done);
      });
    });

    after(function(done) {
      if (!db) {
        return done();
      }
      db.dropCollection('haystack', function() {
        done();
      });
    });

    it('should should default the sample size to `5`', function(done) {
      let seen = 0;
      sample(db, 'haystack').pipe(
        es.through(
          function(doc) {
            seen++;
            this.emit('data', doc);
          },
          function() {
            this.emit('end');
            expect(seen).to.be.equal(5);
            done();
          }
        )
      );
    });

    it('should allow specifying a query', function(done) {
      const docs = [];
      const options = {
        size: 10,
        query: {
          is_even: 1
        }
      };
      sample(db, 'haystack', options).pipe(
        es.through(
          function(doc) {
            docs.push(doc);
            this.emit('data', doc);
          },
          function() {
            this.emit('end');
            expect(
              docs.filter(function(d) {
                return d.is_even === 1;
              }).length
            ).to.be.equal(options.size);
            done();
          }
        )
      );
    });

    it('should get a sample of 10 documents', function(done) {
      let seen = 0;
      sample(db, 'haystack').pipe(
        es.through(
          function(doc) {
            seen++;
            this.emit('data', doc);
          },
          function() {
            this.emit('end');
            expect(seen).to.be.equal(5);
            done();
          }
        )
      );
    });

    it(
      'should return as many documents as possible if ' +
        'the requested sample size is larger than the ' +
        'collection size',
      function(done) {
        let seen = 0;
        sample(db, 'haystack', {
          size: 2000
        }).pipe(
          es.through(
            function(doc) {
              seen++;
              this.emit('data', doc);
            },
            function() {
              this.emit('end');
              expect(seen).to.be.equal(1000);
              done();
            }
          )
        );
      }
    );
  });

  describe('topology', function() {
    this.timeout(30000);

    let dbPrim;
    let dbSec;
    let clientPrim;
    let clientSec;
    const options = {
      readPreference: ReadPreference.primaryPreferred
    };

    before(function(done) {
      const client = new MongoClient('mongodb://localhost:27018/test');

      client.connect(function(
        err,
        connectedClient
      ) {
        if (err) {
          return done(err);
        }
        clientPrim = connectedClient;
        dbPrim = connectedClient.db('test');
        const docs = _range(0, 100).map(function(i) {
          return {
            _id: 'needle_' + i,
            is_even: i % 2
          };
        });
        dbPrim.collection('haystack').insertMany(docs, function() {
          const client2 = new MongoClient('mongodb://localhost:27018/test');

          client2.connect(function(
            errInsert,
            _client
          ) {
            if (errInsert) {
              return done(errInsert);
            }
            clientSec = _client;
            dbSec = _client.db('test');
            dbSec
              .collection('haystack', options)
              .countDocuments(function(errCount) {
                expect(errCount).to.not.exist;
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
        clientPrim.close();
        clientSec.close();
        done();
      });
    });

    it('should sample correctly when connected to a secondary node', function(done) {
      const opts = {
        size: 5,
        query: {}
      };
      // Get a stream of sample documents from the collection and make sure
      // 5 documents have been returned.
      let count = 0;
      const stream = sample(dbSec, 'haystack', opts);
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
