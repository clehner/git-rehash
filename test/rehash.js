var tape = require('tape')
var rehash = require('../')
var repo = require('./repo')
var pull = require('pull-stream')
var crypto = require('crypto')

var objectIds = Object.keys(repo)
var objects = objectIds.map(function (id) { return repo[id] })

function objectEncoding(obj) {
  return obj.type == 'tree' ? 'base64' : 'utf8'
}

function expandObjects() {
  return function (readObject) {
    return function (abort, cb) {
      readObject(abort, function (end, obj) {
        cb(end, obj && {
          type: obj.type,
          length: obj.length,
          read: pull.once(new Buffer(obj.data, objectEncoding(obj)))
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
              data: Buffer.concat(bufs).toString(objectEncoding(obj))
            })
          })
        )
      })
    }
  }
}

function hash(type, data, encoding) {
  return crypto.createHash(type).update(data).digest(encoding)
}

function lookup(gitHash, cb) {
  if (gitHash in repo)
    cb(null, hash('sha256', repo[gitHash].data))
  else
    cb(new Error('hash not present'))
}


function objectsEquals(t) {
  var i = 0
  return function gotObject(obj) {
    t.deepEquals(obj, objects[i], 'got object ' + objectIds[i])
    i++
  }
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
  var gotObject = objectsEquals(t)
  pull(
    pull.values(objects),
    expandObjects(),
    rehash.fromGit('sha256', lookup),
    rehash.toGit('sha256', 32),
    flattenObjects(),
    pull.drain(function (obj) {
      gotObject(obj)
    }, function (err) {
      t.error(err, 'rewrite and flatten objects')
      t.end()
    })
  )
})
