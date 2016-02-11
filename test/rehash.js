var tape = require('tape')
var rehash = require('../')
var repo = require('./repo')
var pull = require('pull-stream')
var crypto = require('crypto')

var objectIds = Object.keys(repo)
var objects = objectIds.map(function (id) { return repo[id] })

function compareObjects(a, b) {
  return a.data > b.data
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
  console.error('lookup', gitHash)
  if (gitHash in repo)
    cb(null, hash('sha256', repo[gitHash].data))
  else
    cb(new Error('hash not present: ' + gitHash))
}


function objectsEquals(t, objs) {
  objs = objs || objects
  var i = 0
  return function gotObject(obj) {
    t.deepEquals(obj, objs[i], 'got ' + objs[i].type) // + ' ' + objectIds[i])
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
    rehash.fromGit('sha256', lookup),
    rehash.toGit('sha256', 32),
    flattenObjects(),
    pull.collect(function (err, objs) {
      t.error(err, 'rewrite and flatten objects')
      // sortObjects(objects).forEach(gotObject)
      t.equals(objs.length, objects.length)
      // t.deepEquals(sortObjects(objs), )
      // console.error(objs)
      t.end()
    })
  )
})
