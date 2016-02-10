var tape = require('tape')
var rehash = require('../')
var repo = require('./repo')
var pull = require('pull-stream')

function objects() {
  var ids = Object.keys(repo)
  var i = 0
  return function (abort, cb) {
    if (i >= ids.length) return cb(true)
    cb(null, repo[ids[i++]])
  }
}

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
    objects(),
    expandObjects(),
    flattenObjects(),
    pull.collect(function (err, objs) {
      t.error(err, 'rewrite and flatten objects')
      var objs2 = Object.keys(repo).map(function (id) { return repo[id] })
      t.deepEqual(objs, objs2, 'the right objects')
      t.end()
    })
  )
})
