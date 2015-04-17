# mongodb-collection-sample

[![build status](https://secure.travis-ci.org/mongodb-js/mongodb-collection-sample.png)](http://travis-ci.org/mongodb-js/mongodb-collection-sample)

Sample documents from a MongoDB collection.

## Install

```
npm install -g mongodb-collection-sample
```

## Example

After installing, you'll have a new method available for collections, `.sample()`.  Start up `mongo` shell and try the following:

```javascript
// Returns up to 5 documents from the user collection.
db.users.sample()

// Returns up to 4 documents from the user collection.
db.users.sample(4)

// Returns up to 3 documents from the user collection where username
// starts with L.
db.users.sample(3, {username: /^L/})

// Returns up to 2 documents from the user collection where username
// starts with L and only the username field.
db.users.sample(2, {username: /^L/}, {username: 1})
```

## License

Apache 2
