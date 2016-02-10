var crypto = require('crypto')
var buffered = require('pull-buffered')

function createHash(type) {
  return function hasher(read) {
    var hash = hasher.hash =
      (typeof type == 'string') ? crypto.createHash(type) : type
    return function (abort, cb) {
      read(abort, function (end, data) {
        if (end === true) hasher.digest = hash.digest()
        else if (!end) hash.update(data)
        cb(end, data)
      })
    }
  }
}

function createGitObjectHash(objectType, objectLength) {
  var hasher = createHash('sha1')
  hasher.hash.update(objectType + ' ' + objectLength + '\0')
  return hasher
}

function rewriteObjectsFromGit(algorithm, lookup) {
  var hashCache = {}

  return function (readObject) {
    var ended
    return function (abort, cb) {
      if (ended) return cb(ended)
      readObject(abort, function (end, obj) {
        if (ended = end) return cb(ended)
        switch (obj.type) {
          case 'blob':
            // blobs do not need to be rewritten
            return cb(null, obj)
          case 'tag':
            rewrite = rewriteTagFromGit(cb)
            break
          case 'tree':
            rewrite = rewriteTreeFromGit(cb)
            break
          case 'commit':
            rewrite = rewriteCommitFromGit(cb)
            break
          default:
            return cb(new Error('Unknown object type ' + obj.type))
        }

        cb(null, {
          type: obj.type,
          length: obj.length,
          read: rewrite(obj.read)
        })
      })
    }
  }

  function lookupCached(gitHash, cb) {
    if (gitHash in hashCache)
      return cb(null, hashCache[gitHash])
    lookup(gitHash, function (err, hash) {
      hashCache[gitHash] = hash
      cb(err, hash)
    })
  }

  function rewriteTagFromGit(cb) {
    return function (read) {
      return function (abort, cb) {
        read(abort, cb)
      }
    }
  }

  function rewriteCommitFromGit(cb) {
    return function (read) {
      var b = buffered(read)
      var readLine = b.lines
      var inBody = false
      return function (abort, cb) {
        if (inBody) return b.passthrough(abort, cb)
        readLine(abort, function (end, line) {
          if (end) return cb(end)

          if (line === '') {
            inBody = true
            return cb(null, new Buffer('\n'))
          }

          // put the other hash after the git hash
          var args = line.split(' ')
          if (args[0] === 'tree' || args[0] === 'parent')
            return lookupCached(args[1], function (err, hash) {
              args[1] += ':' + hash
              cb(err, new Buffer(args.join(' ') + '\n'))
            })

          cb(null, new Buffer(line + '\n'))
        })
      }
    }
  }

  function rewriteTreeFromGit(cb) {
    return function (read) {
      return function (abort, cb) {
        read(abort, cb)
      }
    }
  }
}

function rewriteObjectsToGit(algorithm) {
  return function (readObject) {
    var ended
    return function (abort, cb) {
      if (ended) return cb(ended)
      readObject(abort, function (end, obj) {
        if (ended = end) return cb(ended)
        switch (obj.type) {
          case 'blob':
            return cb(null, obj)
          case 'tag':
            rewrite = rewriteTagToGit(cb)
            break
          case 'tree':
            rewrite = rewriteTreeToGit(cb)
            break
          case 'commit':
            rewrite = rewriteCommitToGit(cb)
            break
          default:
            return cb(new Error('Unknown object type ' + obj.type))
        }

        cb(null, {
          type: obj.type,
          length: obj.length,
          read: rewrite(obj.read)
        })
      })
    }
  }

  function rewriteTagToGit(cb) {
    return function (read) {
      return function (abort, cb) {
        read(abort, cb)
      }
    }
  }

  function rewriteCommitToGit(cb) {
    return function (read) {
      var b = buffered(read)
      var readLine = b.lines
      var inBody = false
      return function (abort, cb) {
        if (inBody) return b.passthrough(abort, cb)
        readLine(abort, function (end, line) {
          if (end) return cb(end)

          if (line === '') {
            inBody = true
            return cb(null, new Buffer('\n'))
          }

          // remove the other hash and leave the git hash
          var args = line.split(' ')
          if (args[0] === 'tree' || args[0] === 'parent') {
            args[1] = args[1].split(':')[0]
            line = args.join(' ')
          }

          cb(null, new Buffer(line + '\n'))
        })
      }
    }
  }

  function rewriteTreeToGit(cb) {
    return function (read) {
      return function (abort, cb) {
        read(abort, cb)
      }
    }
  }
}

exports.fromGit = rewriteObjectsFromGit
exports.toGit = rewriteObjectsToGit
