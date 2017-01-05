'use strict';

const assert = require('chai').assert;
const couchbase = require('couchbase-promises');
const cb = couchbase.Mock;

const Store = require('../../lib/store');

describe('Store', () => {
  let cluster, bucket, store, docs, options;

  beforeEach((done) => {
    cluster = new cb.Cluster('couchbase://localhost');
    bucket = cluster.openBucket('default');
    store = new Store({ bucket: bucket });
    docs = new Map();
    docs.set('a', { foo: 'bar' });
    docs.set('b', { baz: 'qux' });
    options = { ttl: 5000 };
    bucket.insert('c', { quux: 'quuz' }, (err, res) => {
      done();
    });
  });

  describe('#constructor', () => {
    it('should throw if no options', () => {
      assert.throws(() => {
        const test = new Store();
      }, TypeError);
    });

    it('should throw if options not object', () => {
      assert.throws(() => {
        const test = new Store(42);
      }, TypeError);
    });

    it('should throw if no bucket key', () => {
      assert.throws(() => {
        const test = new Store({});
      }, TypeError);
    });

    it('should throw if bucket not defined', () => {
      assert.throws(() => {
        const test = new Store({ bucket: undefined });
      }, TypeError);
    });

    it('should throw if bucket has no insertMultiAsync() method', () => {
      assert.throws(() => {
        const test = new Store({
          bucket: {
            removeMultiAsync: function() {},
            touchMultiAsync: function() {}
          }
        });
      }, TypeError);
    });

    it('should throw if bucket has no touchMultiAsync() method', () => {
      assert.throws(() => {
        const test = new Store({
          bucket: {
            insertMultiAsync: function() {},
            removeMultiAsync: function() {}
          }
        });
      }, TypeError);
    });

    it('should throw if bucket has no removeMultiAsync() method', () => {
      assert.throws(() => {
        const test = new Store({
          bucket: {
            insertMultiAsync: function() {},
            touchMultiAsync: function() {}
          }
        });
      }, TypeError);
    });
  });

  describe('#insert', () => {
    it('should throw if callback not func', () => {
      assert.throws(() => {
        store.insert(docs, options, 42);
      }, TypeError);
    });

    it('should resolve with summary', (done) => {
      store.insert(docs, options, (err, res) => {
        assert.instanceOf(res, Map);
        done();
      });
    });

    it('should set err with undefined on success', (done) => {
      store.insert(docs, options, (err, res) => {
        assert.isNotOk(err);
        done();
      });
    });

    it('should resolve with err on catastrophic failure', (done) => {
      const testBucket = {
        removeMultiAsync: () => {},
        touchMultiAsync: () => {},
        insertMultiAsync: () => {
          return Promise.reject(new Error('Nope'));
        }
      };

      const testStore = new Store({ bucket: testBucket });

      testStore.insert(docs, options, (err, res) => {
        assert.isOk(err);
        done();
      });
    });
  });

  describe('#insertAsync', () => {
    it('should return a Promise', (done) => {
      const result = store.insertAsync(docs, options);
      assert.instanceOf(result, couchbase.Promise);
      done();
    });

    it('should throw if docs not Map', () => {
      assert.throws(() => {
        store.insertAsync(42, options);
      }, TypeError);
    });

    it('should throw if key not string', () => {
      assert.throws(() => {
        docs.set(42, { quux: 'quuz' });
        store.insertAsync(docs, options);
      }, TypeError);
    });

    it('should throw if key length zero', () => {
      assert.throws(() => {
        docs.set('', { quux: 'quuz' });
        store.insertAsync(docs, options);
      }, TypeError);
    });

    it('should throw if no options', () => {
      assert.throws(() => {
        store.insertAsync(docs);
      }, TypeError);
    });

    it('should throw if options not object', () => {
      assert.throws(() => {
        store.insertAsync(docs, 42);
      }, TypeError);
    });

    it('should throw if no options.ttl', () => {
      assert.throws(() => {
        store.insertAsync(docs, {});
      }, TypeError);
    });

    it('should throw if options.ttl not number', () => {
      assert.throws(() => {
        store.insertAsync(docs, { ttl: 'blorg' });
      }, TypeError);
    });

    it('should resolve with instance of Map', (done) => {
      store.insertAsync(docs, options)
        .then((res) => {
          assert.instanceOf(res, Map);
          done();
        });
    });

    it('should resolve with Map with key for each insert', (done) => {
      store.insertAsync(docs, options)
        .then((res) => {
          assert.isTrue(res.has('a'));
          assert.isTrue(res.has('b'));
          done();
        });
    });

    it('should resolve with Map with summary for each insert', (done) => {
      store.insertAsync(docs, options)
        .then((res) => {
          assert.isObject(res.get('a'));
          assert.isObject(res.get('b'));
          done();
        });
    });

    it('should resolve with summary.etag=entry version', (done) => {
      store.insertAsync(docs, options)
        .then((res) => {
          assert.isString(res.get('a').etag);
          assert.isString(res.get('b').etag);
          done();
        });
    });

    it('should resolve with summary.succes=true on success', (done) => {
      store.insertAsync(docs, options)
        .then((res) => {
          assert.isTrue(res.get('a').success);
          assert.isTrue(res.get('b').success);
          done();
        });
    });

    it('should resolve with summary.isCollision=false on success', (done) => {
      store.insertAsync(docs, options)
        .then((res) => {
          assert.isFalse(res.get('a').isCollision);
          assert.isFalse(res.get('b').isCollision);
          done();
        });
    });

    it('should resolve with summary.err=undefined on success', (done) => {
      store.insertAsync(docs, options)
        .then((res) => {
          assert.isNotOk(res.get('a').err);
          assert.isNotOk(res.get('b').err);
          done();
        });
    });

    it('should resolve with summary.success=false on failure', (done) => {
      docs.set('c', 42);
      store.insertAsync(docs, options)
        .then((res) => {
          assert.isTrue(res.get('a').success);
          assert.isTrue(res.get('b').success);
          assert.isFalse(res.get('c').success);
          done();
        });
    });

    it('should resolve with summary.isCollision=true on collision', (done) => {
      docs.set('c', 42);
      store.insertAsync(docs, options)
        .then((res) => {
          assert.isFalse(res.get('a').isCollision);
          assert.isFalse(res.get('b').isCollision);
          assert.isTrue(res.get('c').isCollision);
          done();
        });
    });

    it('should resolve with summary.err=err on failure', (done) => {
      docs.set('c', 42);
      store.insertAsync(docs, options)
        .then((res) => {
          assert.isNotOk(res.get('a').err);
          assert.isNotOk(res.get('b').err);
          assert.isOk(res.get('c').err);
          done();
        });
    });
  });

  describe('#remove', () => {
    it('should throw if callback not func', () => {
      assert.throws(() => {
        store.remove([ 'c' ], 42);
      }, TypeError);
    });

    it('should resolve with summary', (done) => {
      store.remove([ 'c' ], (err, res) => {
        assert.isObject(res);
        assert.isArray(res.succeeded);
        assert.isArray(res.failed);
        done();
      });
    });

    it('should set err with undefined on success', (done) => {
      store.remove([ 'c' ], (err, res) => {
        assert.isNotOk(err);
        done();
      });
    });

    it('should resolve with err on catastrophic failure', (done) => {
      const testBucket = {
        insertMultiAsync: () => {},
        touchMultiAsync: () => {},
        removeMultiAsync: () => {
          return Promise.reject(new Error('Nope'));
        }
      };

      const testStore = new Store({ bucket: testBucket });

      testStore.remove([ 'c' ], (err, res) => {
        assert.isOk(err);
        done();
      });
    });
  });

  describe('#removeAsync', () => {
    it('should throw if keys not array or Set', () => {
      assert.throws(() => {
        store.removeAsync(42);
      }, TypeError);
    });

    it('should remove keys with array', (done) => {
      store.removeAsync([ 'c' ])
        .then((res) => {
          return bucket.getAsync('c');
        })
        .then((res) => {
          assert.fail();
          done();
        })
        .catch((err) => {
          assert.strictEqual(err.code, couchbase.errors.keyNotFound);
          done();
        });
    });

    it('should remove keys with Set', (done) => {
      store.removeAsync(new Set([ 'c' ]))
        .then((res) => {
          return bucket.getAsync('c');
        })
        .then((res) => {
          assert.fail();
          done();
        })
        .catch((err) => {
          assert.strictEqual(err.code, couchbase.errors.keyNotFound);
          done();
        });
    });

    it('should resolve with summary', (done) => {
      store.removeAsync([ 'c' ])
        .then((res) => {
          assert.isObject(res);
          done();
        });
    });

    it('should resolve with summary containing all succeeded keys', (done) => {
      store.removeAsync([ 'c' ])
        .then((res) => {
          assert.include(res.succeeded, 'c');
          done();
        });
    });

    it('should resolve keys that did not exist as succeeded', (done) => {
      store.removeAsync([ 'a' ])
        .then((res) => {
          assert.include(res.succeeded, 'a');
          done();
        });
    });

    it('should resolve with summary containing all failed keys', (done) => {
      const testBucket = {
        touchMultiAsync: () => {},
        insertMultiAsync: () => {},
        removeMultiAsync: () => {
          return Promise.resolve({
            hasErrors: true,
            keys: [ 'a' ],
            results: {
              a: {
                success: false,
                err: { code: 42 }
              }
            }
          });
        }
      };

      const testStore = new Store({ bucket: testBucket });

      testStore.removeAsync([ 'a' ])
        .then((res) => {
          assert.include(res.failed, 'a');
          done();
        });
    });
  });

  describe('#touch', () => {
    it('should throw if callback not func', () => {
      assert.throws(() => {
        store.touch([ 'c' ], options, 42);
      }, TypeError);
    });

    it('should resolve with summary as instance of Map', (done) => {
      store.touch([ 'c' ], options, (err, res) => {
        assert.instanceOf(res, Map);
        done();
      });
    });

    it('should set err with undefined on success', (done) => {
      store.touch([ 'c' ], options, (err, res) => {
        assert.isNotOk(err);
        done();
      });
    });

    it('should resolve with err on catastrophic failure', (done) => {
      const testBucket = {
        insertMultiAsync: () => {},
        removeMultiAsync: () => {},
        touchMultiAsync: () => {
          return Promise.reject(new Error('Nope'));
        }
      };

      const testStore = new Store({ bucket: testBucket });

      testStore.touch([ 'c' ], options, (err, res) => {
        assert.isOk(err);
        done();
      });
    });
  });

  describe('#touchAsync', () => {
    it('should throw if keys not array or Set', () => {
      assert.throws(() => {
        store.touchAsync(42, options);
      }, TypeError);
    });

    it('should throw if key not string', () => {
      assert.throws(() => {
        store.touchAsync([ 42 ], options);
      }, TypeError);
    });

    it('should throw if key length zero', () => {
      assert.throws(() => {
        store.touchAsync([ '' ], options);
      }, TypeError);
    });

    it('should throw if no options', () => {
      assert.throws(() => {
        store.touchAsync([ 'c' ], undefined);
      }, TypeError);
    });

    it('should throw if options not object', () => {
      assert.throws(() => {
        store.touchAsync([ 'c' ], 42);
      }, TypeError);
    });

    it('should throw if no options.ttl', () => {
      assert.throws(() => {
        store.touchAsync([ 'c' ], {});
      }, TypeError);
    });

    it('should throw if options.ttl not number', () => {
      assert.throws(() => {
        store.touchAsync([ 'c' ], { ttl: 'blorg' });
      }, TypeError);
    });

    it('should touch document with new TTL', (done) => {
      const testBucket = {
        removeMultiAsync: () => {},
        insertMultiAsync: () => {},
        touchMultiAsync: (keys) => {
          const expiry = keys.get('c').expiry;
          assert.strictEqual(expiry, parseInt(options.ttl / 1000));
          return Promise.resolve({
            hasErrors: true,
            keys: [ 'c' ],
            results: {
              c: {
                result: { cas: 'test' },
                success: true
              }
            }
          });
        }
      };

      const testStore = new Store({ bucket: testBucket });

      testStore.touchAsync([ 'c' ], options)
        .then((res) => {
          done();
        });
    });

    it('should set summary.etag=cas on successful keys', (done) => {
      store.touchAsync([ 'c' ], options)
        .then((res) => {
          assert.isString(res.get('c').etag);
          done();
        });
    });

    it('should set summary.etag=undefined on failed key', (done) => {
      store.touchAsync([ 'b' ], options)
        .then((res) => {
          assert.isNotOk(res.get('b').etag);
          done();
        });
    });

    it('should set summary.succes=true on successful keys', (done) => {
      store.touchAsync([ 'c' ], options)
        .then((res) => {
          assert.isTrue(res.get('c').success);
          done();
        });
    });

    it('should set summary.sucess=false on failed keys', (done) => {
      store.touchAsync([ 'b' ], options)
        .then((res) => {
          assert.isFalse(res.get('b').success);
          done();
        });
    });

    it('should set summary.isMissing=false on success', (done) => {
      store.touchAsync([ 'c' ], options)
        .then((res) => {
          assert.isFalse(res.get('c').isMissing);
          done();
        });
    });

    it('should set summary.isMissing=true when key missing', (done) => {
      store.touchAsync([ 'b' ], options)
        .then((res) => {
          assert.isTrue(res.get('b').isMissing);
          done();
        });
    });

    it('should set summary.err=err on failure', (done) => {
      store.touchAsync([ 'b' ], options)
        .then((res) => {
          assert.isOk(res.get('b').err);
          done();
        });
    });
  });

});
