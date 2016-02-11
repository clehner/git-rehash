var crypto = require('crypto')
var buffered = require('pull-buffered')
var pull = require('pull-stream')
var cat = require('pull-cat')
var pushable = require('pull-pushable')

function createHash(type, onEnd) {
  function hasher(read) {
    return function (abort, cb) {
      read(abort, function (end, data) {
        if (end === true) hasher.digest = hash.digest()
        else if (!end) hash.update(data)
        cb(end, data)
        if (end && onEnd) onEnd(end === true ? null : end, hasher.digest)
      })
    }
  }
  var hash = hasher.hash =
    (typeof type == 'string') ? crypto.createHash(type) : type
  return hasher
}

function createGitObjectHash(objectType, objectLength, onEnd) {
  var hasher = createHash('sha1', onEnd)
  hasher.hash.update(objectType + ' ' + objectLength + '\0')
  return hasher
}

function rewriteObjectsFromGit(algorithm, lookup) {
  var hashCache = {}
  var waitingForHash = {}
  var outQueue = pushable(function (err) {
    console.error('out queue done', err)
  })

  return function (readObject) {
    var ended
    var readyToResolve

    return cat([
      readOut,
      outQueue
    ])

    function readOut(abort, cb) {
      if (ended) return cb(ended)
      readObject(abort, function next(end, obj) {
        if (end === true) readyToResolve = true
        if (ended = end) return cb(ended)
        var hasherIn = createGitObjectHash(obj.type, obj.length, inDone)
        var hasherOut = createHash(algorithm, outDone)
        var queue = false

        console.error('object', obj.type)

        // Pass through blobs. Commits, tags, and trees don't call back until
        // they are finished.
        switch (obj.type) {
          case 'blob':
            return cb(null, {
              type: obj.type,
              length: obj.length,
              read: pull(
                obj.read,
                hasherIn,
                hasherOut
              )
            })
          case 'commit':
          case 'tag':
            rewrite = rewriteCommitOrTagFromGit(hasherIn)
            break
          case 'tree':
            rewrite = rewriteTreeFromGit(hasherIn)
            break
          default:
            return cb(new Error('Unknown object type ' + obj.type))
        }

        // console.error('pulling')
        queue = true
        pull(
          obj.read,
          hasherIn,
          rewrite,
          hasherOut,
          pull.collect(function (err, bufs) {
            // console.error('collected', err)
            if (err) return outQueue.end(err)
            if (bufs.length == 0)
              throw new Error('no bufs ' + err + ' ' +
                hasherIn.digest.toString('hex') + ' ' + hasherOut.digest)
            outQueue.push({
              type: obj.type,
              length: obj.length,
              read: pull.values(bufs)
            })
          })
        )

        function inDone(err) {
          // console.error('in done', err)
          if (err) return cb(err)
          var gitHash = hasherIn.digest.toString('hex')
          // console.error('git hash', gitHash)
          // Mark the git hash as a placeholder so we know that we have this
          // object but it is waiting for some other objects
          if (!(gitHash in hashCache))
            hashCache[gitHash] = null
          if (queue)
            readObject(null, next)
        }

        function outDone(err, digest) {
          if (err) console.error('out done err', err)
          if (err) return cb(err)
          var gitHash = hasherIn.digest.toString('hex')
          var outHash = digest
          // console.error('out done', gitHash, '->', outHash)
          if (gitHash == 'e0ceccfa415db4c7eef667b59dec7b3db8d62e25')
            console.error('got here is file blob tree', gitHash, outHash)
          hashCache[gitHash] = outHash
          var cbs = waitingForHash[gitHash]
          if (cbs) {
            while (cbs.length)
              cbs.pop()(null, outHash)
            delete waitingForHash[gitHash]
          }
          /*
          if (readyToResolve) {
            readyToResolve = false
            console.log('resolve', waitingForHash, hashCache)
            resolve()
          }
          */
        }
      })
    }
  }

  function resolve() {
    // request lookup for remaining unknown git hashes
    // console.log('resolve', typeof waitingForHash, waitingForHash)
    for (var gitHash in waitingForHash)
      if (!(gitHash in hashCache)) {
        console.error('LOOKUP!', gitHash)
        return lookup(gitHash, function (err, hash) {
          hashCache[gitHash] = hash
          for (var cbs = waitingForHash[gitHash]; cbs.length; )
            cbs.pop()(err, hash)
          delete waitingForHash[gitHash]
          resolve()
        })
      }
    outQueue.end()
  }

  function lookupCached(gitHash, cb) {
    var hash = hashCache[gitHash]
    if (hash)
      return cb(null, hash)
    if (1) {
      // console.log('waiting', gitHash)
      ;(waitingForHash[gitHash] || (waitingForHash[gitHash] = [])).push(cb)
    } else {
      lookup(gitHash, function (err, hash) {
        hashCache[gitHash] = hash
        cb(err, hash)
      })
    }
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
                var hash = hashCache[args[1]]
                if (hash) {
                  args.push(hash.toString('hex'))
                  lines[i] = args.join(' ')
                } else {
                  return lookupCached(args[1], function (err, hash) {
                    if (err) return cb(err)
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

  // Trees, commits and tags are buffered completely because they may refer to
  // objects which are not available yet. The tree's output should not block
  // because then the sink may not read the next object. Only blobs get passed
  // through without buffering (or rewriting), since they don't point to any
  // other objects.
  function rewriteTreeFromGit(hasherIn) {
    return function (read) {
      var ended, bufsOut = []

      return function (abort, cb) {
        if (ended) return cb(ended)
        pull.collect(function (err, bufs) {
          if (err) return cb(ended = err)
          processBuf(Buffer.concat(bufs), 0)
        })(read)

        function processBuf(buf, i) {
          if (i >= buf.length) {

          if (hasherIn.digest.toString('hex') ==
            'e0ceccfa415db4c7eef667b59dec7b3db8d62e25')
            console.error('done is file tree', bufsOut.length)

            ended = true
            cb(null, Buffer.concat(bufsOut))
            return
          }

          var j = buf.indexOf(0, i, 'ascii')
          // pass through file info, null byte, and git hash
          bufsOut.push(buf.slice(i, j + 21))
          var gitHash = buf.slice(j + 1, j + 21)

          if (hasherIn.digest.toString('hex') ==
            'e0ceccfa415db4c7eef667b59dec7b3db8d62e25')
            console.error('here is file tree', gitHash.toString('hex'))

          lookupCached(gitHash.toString('hex'), function (err, hash) {
          if (hasherIn.digest.toString('hex') ==
            'e0ceccfa415db4c7eef667b59dec7b3db8d62e25')
              console.error('looked up here is file tree', hash, buf.length, j + 21)
            if (err) return cb(err)
            // append other hash
            bufsOut.push(hash)
            processBuf(buf, j + 21)
          })
        }
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
