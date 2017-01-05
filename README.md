# landlord-couchbase

A [`Store`](https://www.npmjs.com/package/landlord#stores) implementation for the [`landlord`](https://www.npmjs.com/package/landlord) module using [Couchbase](http://couchbase.com).

## Usage

Add `landlord-couchbase` as a dependency in `package.json`:

```sh
$ npm install landlord-couchbase -S
```

Then configure instances of `Landlord` to use `landlord-couchbase` as it's store:

```js
const couchbase = require('couchbase-promises');
const Landlord = require('landlord');
const Store = require('landlord-couchbase');

const cluster = new couchbase.Cluster('couchbase://127.0.0.1');
const bucket = cluster.openBucket('default');

const store = new Store({ bucket: bucket });

const landlord = new Landlord({
  store: store
});
```

## Couchbase

This module expects [`couchbase-promises`](https://www.npmjs.com/package/couchbase-promises)-styled  `Bucket` instances.  This is because `couchbase-promises` provides a number of "multi" operation methods for batching document mutations.  At a minimum, `landlord-couchbase` expects buckets to have the methods: `insertMultiAsync()`, `removeMultiAsync()`, and `touchMultiAsync()`.
