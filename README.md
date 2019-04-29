# mongodb-collection-sample [![][npm_img]][npm_url] [![][travis_img]][travis_url]

> Sample documents from a MongoDB collection.

## Install

```
npm install --save mongodb-collection-sample
```

## Example

```
npm install mongodb lodash mongodb-collection-sample
```

```javascript
var sample = require('mongodb-collection-sample');
var mongodb = require('mongodb');
var _ = require('lodash');

// Connect to mongodb
mongodb.connect('mongodb://localhost:27017', function(err, db){
  if(err){
    console.error('Could not connect to mongodb:', err);
    return process.exit(1);
  }

  // Generate 1000 documents
  var docs = _range(0, 1000).map(function(i) {
    return {
      _id: 'needle_' + i,
      is_even: i % 2
    };
  });

  // Insert them into a collection
  db.collection('haystack').insert(docs, function(err){
    if(err){
      console.error('Could not insert example documents', err);
      return process.exit(1);
    }

    var options = {};
    // Size of the sample to capture [default: `5`].
    options.size = 5;

    // Query to restrict sample source [default: `{}`]
    options.query = {};

    // Get a stream of sample documents from the collection.
    var stream = sample(db, 'haystack', options);
    stream.on('error', function(err){
      console.error('Error in sample stream', err);
      return process.exit(1);
    });
    stream.on('data', function(doc){
      console.log('Got sampled document `%j`', doc);
    });
    stream.on('end', function(){
      console.log('Sampling complete!  Goodbye!');
      db.close();
      process.exit(0);
    });
  });
});
```

## Options

Supported options that can be passed to `sample(db, coll, options)` are

- `query`: the filter to be used, default is `{}`
- `size`: the number of documents to sample, default is `5`
- `fields`: the fields you want returned (projection object), default is `null`
- `raw`: boolean to return documents as raw BSON buffers, default is `false`
- `sort`: the sort field and direction, default is `{_id: -1}`
- `maxTimeMS`: the maxTimeMS value after which the operation is terminated, default is `undefined`
- `promoteValues`: boolean whether certain BSON values should be cast to native Javascript values or not. Default is `true`


## How It Works

#### Native Sampler

MongoDB version 3.1.6 and above generally uses the `$sample` aggregation operator:

```
db.collectionName.aggregate([
  {$match: <query>},
  {$sample: {size: <size>}},
  {$project: <fields>},
  {$sort: <sort>}
])
```

However, if more documents are requested than are available, the `$sample` stage
is omitted for performance optimization. If the sample size is above 5% of the
result set count (but less than 100%), the algorithm falls back to the reservoir
sampling, to avoid a blocking sort stage on the server.


#### Reservoir Sampling

For MongoDB version 3.1.5 and below we use a client-size reservoir sampling algorithm.

- Query for a stream of _id values, limit 10,000.
- Read stream of `_id`s and save `sampleSize` randomly chosen values.
- Then query selected random documents by _id.

The two modes, illustrated:

[![][sampling_post_316_png]][sampling_post_316_svg]
[![][sampling_pre_316_png]][sampling_pre_316_svg]

## Performance Notes

For peak performance of the client-side reservoir sampler, keep the following guidelines in mind.

- The initial query for a stream of `_id` values must be limited to some finite value. (Default 10k)
- This query should be covered by an index
- Since there's a limit, you may wish to bias for recent documents via a sort. (Default: {_id: -1})
- [Don't sort on {$natural: -1}](https://docs.mongodb.org/manual/reference/operator/meta/natural): this forces a collection scan!

> Queries that include a sort by $natural order do not use indexes to fulfill the query predicate

- When retrieving docs: batch using one $in to reduce network chattiness.

## License

Apache 2

[travis_img]: https://secure.travis-ci.org/mongodb-js/collection-sample.svg?branch=master
[travis_url]: https://travis-ci.org/mongodb-js/collection-sample
[npm_img]: https://img.shields.io/npm/v/mongodb-collection-sample.svg
[npm_url]: https://www.npmjs.org/package/mongodb-collection-sample
[gitter_img]: https://badges.gitter.im/Join%20Chat.svg
[sampling_post_316_png]: docs/sampling_analyzing_post_316.png?raw=true
[sampling_post_316_svg]: docs/sampling_analyzing_post_316.svg
[sampling_pre_316_png]: docs/sampling_analyzing_pre_316.png?raw=true
[sampling_pre_316_svg]: docs/sampling_analyzing_pre_316.svg
