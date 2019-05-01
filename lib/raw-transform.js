var Transform = require('stream').Transform;
var BSON = require('bson');

module.exports = function rawTransform(raw) {
  // split up incoming raw bytes and return documents one by one; NODE-1906
  // this is only necessary for native sampler
  function transform(chunk, encoding, cb) {
    // only transform for raw bytes
    if (raw) {
      var response = BSON.deserialize(chunk);
      response.cursor.firstBatch.forEach(function(doc) {
        this.push(BSON.serialize(doc));
      }.bind(this));
      return cb();
    }
    // otherwise go back to the main stream
    return cb(null, chunk);
  }

  return new Transform({
    readableObjectMode: true,
    writableObjectMode: true,
    transform: transform
  });
};
