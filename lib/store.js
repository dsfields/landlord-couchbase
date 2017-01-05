'use strict';

const elv = require('elv');

const msg = {
  noOptions: 'Arg "options" is required',
  optionsObj: 'Arg "options" must be an object',
  optionsNoBucket: 'Arg "options" must specify a "bucket"',
  noBucket: 'Arg options.bucket cannot be null or undefined',
  noInsert: 'Arg options.bucket must have an insertMultiAsync() method ',
  noTouch: 'Arg options.bucket must have a touchMultiAsync() method',
  noRemove: 'Arg options.bucket must have a removeMultiAsync() method',
  docsNotMap: 'Arg "docs" must be a Map',
  invalidKey: 'Keys must be strings, and have a length greater than zero',
  noInOptions: 'Arg "options is required"',
  inOptionsObj: 'Arg "options" must be an object',
  inOptionsTtl: 'Arg "options" requires the key "ttl"',
  inOptionsTtlNum: 'Arg "options" key "ttl" must be a number',
  invalidCallback: 'Arg "callback" must be a function'
};

const assertConstOptions = (options) => {
  if (!elv(options))
    throw new TypeError(msg.noOptions);

  if (typeof options !== 'object')
    throw new TypeError(msg.optionsObj);

  if (!options.hasOwnProperty('bucket'))
    throw new TypeError(msg.optionsNoBucket);

  const bucket = options.bucket;

  if (!elv(bucket))
    throw new TypeError(msg.noBucket);

  if (typeof bucket.insertMultiAsync !== 'function')
    throw new TypeError(msg.noInsert);

  if (typeof bucket.touchMultiAsync !== 'function')
    throw new TypeError(msg.noTouch);

  if (typeof bucket.removeMultiAsync !== 'function')
    throw new TypeError(msg.noRemove);
};

const assertOptions = (options) => {
  if (!elv(options))
    throw new TypeError(msg.noInOptions);

  if (typeof options !== 'object')
    throw new TypeError(msg.inOptionsObj);

  if (!options.hasOwnProperty('ttl'))
    throw new TypeError(msg.inOptionsTtl);

  if (typeof options.ttl !== 'number')
    throw new TypeError(msg.inOptionsTtlNum);

  return {
    expiry: parseInt(options.ttl / 1000)
  }
};

const assertDocs = (docs) => {
  const prepared = new Map();

  if (!(docs instanceof Map))
    throw new TypeError(msg.docsNotMap);

  for (let e of docs) {
    const key = e[0];
    const value = e[1];

    if (typeof key !== 'string' || key.length === 0)
      throw TypeError(msg.invalidKey);

    prepared.set(key, { value: value });
  }

  return prepared;
};

const assertKeys = (keys) => {
  if (!Array.isArray(keys) && !(keys instanceof Set))
    throw new TypeError('Arg "keys" must be an array or Set');
};

const assertTouchKeys = (keys, options) => {
  assertKeys(keys);

  const prepared = new Map();
  const expiry = options.expiry;

  keys.forEach((key, value) => {
    if (typeof key !== 'string' || key.length === 0)
      throw TypeError(msg.invalidKey);

    prepared.set(key, { expiry: expiry });
  });

  return prepared;
};

const assertCallback = (callback) => {
  if (typeof callback !== 'function')
    throw new TypeError(msg.invalidCallback);
};

const KEY_EXISTS  = 12,
      KEY_MISSING = 13;

const me = new WeakMap();

class Store {

  constructor(options) {
    assertConstOptions(options);
    me.set(this, {
      bucket: options.bucket
    });
  }

  insert(docs, options, callback) {
    assertCallback(callback);

    const cb = callback;

    this.insertAsync(docs, options)
      .then((summary) => {
        cb(undefined, summary);
      })
      .catch((err) => {
        cb(err);
      });
  }

  insertAsync(docs, options) {
    const prepared = assertDocs(docs);
    const opt = assertOptions(options);

    return me.get(this).bucket.insertMultiAsync(prepared, opt)
      .then((res) => {
        const summary = new Map();

        for (let i = 0; i < res.keys.length; i++) {
          const key = res.keys[i];
          const val = res.results[key];
          const result = val.result;

          summary.set(key, {
            etag: (val.success) ? result.cas.toString() : undefined,
            success: val.success,
            isCollision: (val.success) ? false : val.err.code === KEY_EXISTS,
            err: val.err
          });
        }

        return summary;
      });
  }

  remove(keys, callback) {
    assertCallback(callback);

    const cb = callback;

    this.removeAsync(keys)
      .then((res) => {
        cb(undefined, res);
      })
      .catch((err) => {
        cb(err);
      });
  }

  removeAsync(keys) {
    assertKeys(keys);

    return me.get(this).bucket.removeMultiAsync(keys)
      .then((res) => {
        const summary = {
          succeeded: [],
          failed: []
        };

        for (let i = 0; i < res.keys.length; i++) {
          const key = res.keys[i];
          const val = res.results[key];

          if (val.success || val.err.code === KEY_MISSING)
            summary.succeeded.push(key);
          else
            summary.failed.push(key);
        }

        return summary;
      });
  }

  touch(keys, options, callback) {
    assertCallback(callback);

    const cb = callback;

    this.touchAsync(keys, options)
      .then((res) => {
        cb(undefined, res);
      })
      .catch((err) => {
        cb(err);
      });
  }

  touchAsync(keys, options) {
    const opt = assertOptions(options);
    const prepared = assertTouchKeys(keys, opt);

    return me.get(this).bucket.touchMultiAsync(prepared)
      .then((res) => {
        const summary = new Map();

        for (let i = 0; i < res.keys.length; i++) {
          const key = res.keys[i];
          const val = res.results[key];
          const result = val.result;

          summary.set(key, {
            etag: (val.success) ? result.cas.toString() : undefined,
            success: val.success,
            isMissing: (val.success) ? false : val.err.code === KEY_MISSING,
            err: val.err
          });
        }

        return summary;
      });
  }

}

module.exports = Store;
