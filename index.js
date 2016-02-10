var crypto = require('crypto')
var buffered = require('pull-buffered')
var pull = require('pull-stream')

function createHash(type, onEnd) {
  function hasher(read) {
    return function (abort, cb) {
      read(abort, function (end, data) {
        if (end === true) hasher.digest = hash.digest()
        else if (!end) hash.update(data)
        cb(end, data)
        if (end && onEnd) onEnd(end === true ? null : end)
      })
    }
  }
  var hash = hasher.hash =
    (typeof type == 'string') ? crypto.createHash(type) : type
  return hasher
}

function createGitObjectHash(objectType, objectLength) {
  var hasher = createHash('sha1')
  hasher.hash.update(objectType + ' ' + objectLength + '\0')
  return hasher
}

function passthrough(onEnd) {
  return function (read) {
    return function (abort, cb) {
      read(abort, function (end, data) {
        if (end && onEnd) onEnd(end)
        cb(end, data)
      })
    }
  }
}

function rewriteObjectsFromGit(algorithm, lookup) {
  var hashCache = {}

  return function (readObject) {
    var ended
    return function (abort, cb) {
      if (ended) return cb(ended)
      readObject(abort, function (end, obj) {
        if (ended = end) return cb(ended)
        var hasherIn = createGitObjectHash(obj.type, obj.length)

        switch (obj.type) {
          case 'blob':
            rewrite = passthrough()
            break
          case 'commit':
          case 'tag':
            rewrite = rewriteCommitOrTagFromGit(hasherIn)
            break
          case 'tree':
            rewrite = rewriteTreeFromGit()
            break
          default:
            return cb(new Error('Unknown object type ' + obj.type))
        }

        var hasherOut = createHash(algorithm, next)

        cb(null, {
          type: obj.type,
          length: obj.length,
          read: pull(
            obj.read,
            hasherIn,
            rewrite,
            hasherOut
          )
        })

        function next(err) {
          if (err) return cb(err)
          // console.error('hashed', hasherIn.digest, 'to', hasherOut.digest)
          hashCache[hasherIn.digest] = hasherOut.digest
        }
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

  function rewriteCommitOrTagFromGit(gitHasher) {
    return function (read) {
      var ended, lines
      return function (abort, cb) {
        if (ended) return cb(ended)
        ended = true

        pull.collect(function (err, bufs) {
          if (err) return cb(ended = err)
          lines = Buffer.concat(bufs).toString('utf8').split('\n')
          lines.unshift('sha1 ' + gitHasher.digest)
          processLines(1)
        })(read)

        function processLines(i) {
          for (; lines[i]; i++) {
            var args = lines[i].split(' ')
            switch (args[0]) {
              case 'tree':
              case 'parent':
              case 'object':
              if (args[1] in hashCache) {
                args.push(hashCache[args[1]].toString('hex'))
                lines[i] = args.join(' ')
              } else {
                return lookupCached(args[1], function (err, hash) {
                  args.push(hash.toString('hex'))
                  lines[i] = args.join(' ')
                  processLines(i+1)
                })
              }
            }
          }

          cb(null, new Buffer(lines.join('\n'), 'utf8'))
        }
      }
    }
  }

  function rewriteTreeFromGit() {
    return function (read) {
      var b = buffered(read)
      var readFileInfo = b.delimited(0)
      var readGitHash = b.chunks(20)

      return function readEntry(abort, cb) {
        readFileInfo(abort, function (end, fileInfo) {
          if (end) return cb(end)
          readGitHash(abort, function (end, gitHash) {
            if (end) return cb(end)
            lookupCached(gitHash.toString('hex'), function (err, hash) {
              if (err) return cb(err)
              cb(null, Buffer.concat([
                new Buffer(fileInfo),
                new Buffer([0]),
                gitHash,
                hash
              ]))
            })
          })
        })
      }
    }
  }
}

function rewriteObjectsToGit(algorithm, hashLength) {
  return function (readObject) {
    var ended
    return function (abort, cb) {
      if (ended) return cb(ended)
      readObject(abort, function (end, obj) {
        if (ended = end) return cb(ended)
        switch (obj.type) {
          case 'blob':
            return cb(null, obj)
          case 'commit':
          case 'tag':
            rewrite = rewriteCommitOrTagToGit(cb)
            break
          case 'tree':
            rewrite = rewriteTreeToGit(cb)
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

  function rewriteCommitOrTagToGit(cb) {
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
          switch (args[0]) {
            case 'tree':
            case 'parent':
            case 'object':
              args.pop()
              line = args.join(' ')
              break
            case 'sha1':
              return cb(null, new Buffer(0))
          }

          cb(null, new Buffer(line + '\n'))
        })
      }
    }
  }

  function rewriteTreeToGit(cb) {
    return function (read) {
      var b = buffered(read)
      var readFileInfo = b.delimited(0)
      var readHashes = b.chunks(20 + hashLength)

      return function readEntry(abort, cb) {
        readFileInfo(abort, function (end, fileInfo) {
          if (end) return cb(end)
          readHashes(abort, function (end, hashes) {
            if (end) return cb(end)
            cb(null, Buffer.concat([
              new Buffer(fileInfo),
              new Buffer([0]),
              hashes.slice(0, 20) // keep only the git hash
            ]))
          })
        })
      }
    }
  }
}

exports.fromGit = rewriteObjectsFromGit
exports.toGit = rewriteObjectsToGit
