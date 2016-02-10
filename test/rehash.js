var tape = require('tape')
var rehash = require('../')
var repo = require('./repo')
var pull = require('pull-stream')

var objectsArray = Object.keys(repo).map(function (id) { return repo[id] })

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
              data: bufs.join('')
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
    pull.values(objectsArray),
    expandObjects(),
    flattenObjects(),
    pull.collect(function (err, objs) {
      t.error(err, 'rewrite and flatten objects')
      t.deepEqual(objs, objectsArray, 'the right objects')
      t.end()
    })
  )
})

tape('rewrite object hashes', function (t) {
  pull(
    pull.values(objectsArray),
    expandObjects(),
    rehash.fromGit('sha256', lookup),
    rehash.toGit('sha256', lookup),
    flattenObjects(),
    pull.collect(function (err, objs) {
      t.error(err, 'rewrite and flatten objects')
      t.deepEqual(objs, objectsArray, 'the right objects')
      t.end()
    })
  )
})
