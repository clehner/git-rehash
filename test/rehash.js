var tape = require('tape')
var rehash = require('../')
var repo = require('./repo')
var pull = require('pull-stream')
var crypto = require('crypto')

var objectIds = Object.keys(repo)
var objects = objectIds.map(function (id) {
  obj = repo[id]
  obj.sha1 = id
  return obj
})

function compareObjects(a, b) {
  return (a.length - b.length)
    || (a.data > b.data ? 1 : a.data < b.data ? -1 : 0)
}

function sortObjects(objs) {
  return objs.slice().sort(compareObjects)
}

function objectEncoding(obj) {
  return obj.type == 'tree' ? 'hex' : 'utf8'
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
            var buf = Buffer.concat(bufs)
            cb(null, {
              type: obj.type,
              length: obj.length,
              data: buf.toString(objectEncoding(obj)),
              sha1: gitHash(obj, buf)
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

function gitHash(obj, data) {
  var hasher = crypto.createHash('sha1')
  hasher.update(obj.type + ' ' + obj.length + '\0')
  hasher.update(data)
  return hasher.digest('hex')
}

function lookup(gitHash, cb) {
  console.error('lookup', gitHash)
  if (gitHash in repo)
    cb(null, hash('sha256', repo[gitHash].data))
  else
    cb(new Error('hash not present: ' + gitHash))
}


function objectsEquals(t, objs) {
  var i = 0
  return function gotObject(obj) {
    t.deepEquals(obj, objs[i], 'got ' + objs[i].type)
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
  var gotObject = objectsEquals(t, sortObjects(objects))
  pull(
    pull.values(objects),
    expandObjects(),
    rehash.fromGit('sha256', function (gitHash, cb) {
      t.fail('lookup should not be needed when all objects are in the stream')
      lookup(gitHash, cb)
    }),
    rehash.toGit(),
    flattenObjects(),
    pull.collect(function (err, objs) {
      t.error(err, 'rewrite and flatten objects')
      t.equals(objs.length, objects.length, 'got the right number of objects')
      sortObjects(objs).forEach(gotObject)
      t.end()
    })
  )
})
