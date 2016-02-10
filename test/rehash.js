var tape = require('tape')
var rehash = require('../')
var repo = require('./repo')
var pull = require('pull-stream')

var objects = Object.keys(repo).map(function (id) {
  var obj = repo[id]
  return {
    type: obj.type,
    length: obj.length,
    data: new Buffer(obj.data)
  }
})

function expandObjects() {
  return function (readObject) {
    return function (abort, cb) {
      readObject(abort, function (end, obj) {
        cb(end, obj && {
          type: obj.type,
          length: obj.length,
          read: pull.once(obj.data)
        })
      })
    }
  }
}

function flattenObjects() {
  return function (readObject) {
    return function (abort, cb) {
      readObject(abort, function (end, obj) {
        if (end) return cb(end)
        pull(
          obj.read,
          pull.collect(function (err, bufs) {
            if (err) return cb(err)
            cb(null, {
              type: obj.type,
              length: obj.length,
              data: Buffer.concat(bufs)
            })
          })
        )
      })
    }
  }
}

function lookup() {
  throw new Error('not impl')
}

tape('pass through', function (t) {
  pull(
    pull.values(objects),
    expandObjects(),
    flattenObjects(),
    pull.collect(function (err, objs) {
      t.error(err, 'rewrite and flatten objects')
      t.deepEqual(objs, objects, 'the right objects')
      t.end()
    })
  )
})

tape('rewrite object hashes', function (t) {
  pull(
    pull.values(objects),
    expandObjects(),
    rehash.fromGit('sha256', lookup),
    rehash.toGit('sha256', lookup),
    flattenObjects(),
    pull.collect(function (err, objs) {
      t.error(err, 'rewrite and flatten objects')
      t.deepEqual(objs, objects, 'the right objects')
      t.end()
    })
  )
})
