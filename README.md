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

## License

Apache 2

[travis_img]: https://secure.travis-ci.org/mongodb-js/mongodb-collection-sample.svg?branch=master
[travis_url]: https://travis-ci.org/mongodb-js/mongodb-collection-sample
[npm_img]: https://img.shields.io/npm/v/mongodb-collection-sample.svg
[npm_url]: https://www.npmjs.org/package/mongodb-collection-sample
[gitter_img]: https://badges.gitter.im/Join%20Chat.svg
