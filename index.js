var crypto = require('crypto')
var buffered = require('pull-buffered')
var pull = require('pull-stream')
var cat = require('pull-cat')
var pushable = require('pull-pushable')
var multicb = require('multicb')

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

function createGitHash(objectType, objectLength, onEnd) {
  var hasher = createHash('sha1', onEnd)
  hasher.hash.update(objectType + ' ' + objectLength + '\0')
  return hasher
}

function rewriteObjectsFromGit(algorithm, lookup) {
  var hashCache = {/* sha1: other_hash */}
  var depends = {/* dest_sha1: {src_sha1: tryResolve} */}
  var outQueue = pushable(function (err) {
    console.error('out queue done', err)
  })
  var waiting = 0

  return function (readObject) {
    var ended

    return cat([
      readOut,
      outQueue
    ])

    function readOut(abort, cb) {
      if (ended) return cb(ended)
      readObject(abort, function next(end, obj) {
        if (end === true) {
          console.error('done reading')
          resolveAll()
        }
        if (ended = end) return cb(ended)
        var hashed = multicb({pluck: 1, spread: true})
        var gitHasher = createGitHash(obj.type, obj.length, hashed())
        var outHasher = createHash(algorithm, hashed())
        hashed()(null, obj.type)
        hashed(gotHashed)
        var rewrite

        // console.error('object', obj.type)

        switch (obj.type) {
          // Pass through blobs
          case 'blob':
            cb(null, {
              type: obj.type,
              length: obj.length,
              read: pull(
                obj.read,
                gitHasher,
                outHasher
              )
            })
            waiting++
            return

          // Commits, tags, and trees have to be buffered and analyzed
          case 'commit':
          case 'tag':
            rewrite = rewriteCommitOrTagFromGit
            break
          case 'tree':
            rewrite = rewriteTreeFromGit
            break
          default:
            return cb(new Error('Unknown object type ' + obj.type))
        }

        // Index the object (find what it links to),
        // and then read the next object.
        // When the current object is done being rewritten,
        // append it to the queue to be read out later
        pull(
          obj.read,
          gitHasher,
          rewrite(obj.type, obj.length, gitHasher, function (err) {
            // Object indexed. Read the next one
            if (err) readObject(err, function (e) { cb(e || err) })
            else readObject(null, next)
          }),
          outHasher,
          pull.collect(function (err, bufs) {
            if (err) return outQueue.end(err)
            outQueue.push({
              type: obj.type,
              length: obj.length,
              read: pull.values(bufs)
            })
            // console.error('waiting', waiting)
          })
        )
      })
    }
  }

  function gotHashed(err, gitDigest, outDigest, type) {
    // console.error('hashed', arguments.length, [].slice.call(arguments))
    if (err) throw new Error(err)
    var gitHash = gitDigest.toString('hex')
    var outHash = outDigest.toString('hex')
    console.error('rewrote', type, gitHash, outHash)
    hashCache[gitHash] = outHash

    // try resolving what be resolved
    var rdeps = depends[gitHash]
    for (var hash in rdeps) {
      var tryResolve = rdeps[hash]
      tryResolve()
    }
    delete depends[gitHash]
    console.error('waiting--:', waiting-1, gitHash)
    if (!--waiting)
      console.error('end'),
      outQueue.end(true)
  }

  function canResolve(links) {
    for (var sha1 in links)
      if (!(sha1 in hashCache))
        return false
    return true
  }

  function resolveAll(err) {
    if (err) throw err
    console.error('resolving all')
    waiting++
    for (var hash in depends) {
      // console.error('hash', hash)
      var rdeps = depends[hash]
      for (var rdep in rdeps) {
        // console.error('rdep', rdep, 'for', hash, hashCache[rdep], hashCache[hash])
        var tryResolve = rdeps[rdep]
        if (hash in hashCache) {
          tryResolve()
        }
      }
    }

    // check for unresolved links
    var thingsRemaining = false
    if (thingsRemaining) {
      // TODO
    }

    if (!--waiting)
      console.error('end'),
      outQueue.end(true)

    console.error('resolve done', waiting)
  }

  function rewriteCommitOrTagFromGit(type, length, gitHasher, onIndexed) {
    var gitHash, lines, links, ended
    var out = pushable()
    var resolving

    return function (read) {
      pull(
        read,
        pull.collect(function (err, bufs) {
          if (err) return onIndexed(err)
          gitHash = gitHasher.digest.toString('hex')
          lines = Buffer.concat(bufs).toString('utf8').split('\n')
          lines.unshift('sha1 ' + gitHash)
          links = indexLines()
          // console.error('indexed', type)
          onIndexed()
        })
      )
      return out
    }

    function indexLines() {
      var links = {/* sha1: [lineNum] */}
      for (var i = 1; lines[i]; i++) {
        var args = lines[i].split(' ')
        switch (args[0]) {
          case 'tree':
          case 'parent':
          case 'object':
            // Record the link from this object to its dependency
            // and the line number of the link so we can rewrite it later
            var sha1 = args[1]
            if (sha1 in links) {
              links[sha1].push(i)
            } else {
              links[sha1] = [i]
              // if the dependency is already rewritten, don't do anything
              // add backlink to resolve function
              ;(depends[sha1] || (depends[sha1] = {}))[gitHash] = tryResolve
            }
        }
      }
      return links
    }

    function tryResolve() {
      if (!canResolve(links) || ended || resolving) return false
      waiting++
      // console.error('waiting++:', waiting, gitHash)
      resolving = true
      // rewrite the links
      for (var sha1 in links) {
        var lineNums = links[sha1]
        var hash = hashCache[sha1]
        for (var i = 0; i < lineNums; i++)
          lines[lineNums[i]] += ' ' + hash
      }
      // console.error('resolved', type, gitHash)
      out.push(new Buffer(lines.join('\n'), 'utf8'))
      out.end(true)
      // console.error('deleting dep', obj.type, !!depends[sha1][gitHash])
      // delete depends[sha1][gitHash]
      return true
    }

  }

  // Trees, commits and tags are buffered completely because they may refer to
  // objects which are not available yet. The tree's output should not block
  // because then the sink may not read the next object. Only blobs get passed
  // through without buffering (or rewriting), since they don't point to any
  // other objects.
  function rewriteTreeFromGit(type, length, gitHasher, onIndexed) {
    var gitHash, buf, links
    var out = pushable()
    var resolving

    return function (read) {
      pull(
        read,
        pull.collect(function (err, bufs) {
          if (err) return onIndexed(err)
          gitHash = gitHasher.digest.toString('hex')
          // TODO: do this incrementally
          buf = Buffer.concat(bufs)
          links = indexLinks()
          // console.error('indexed.', type)
          onIndexed()
        })
      )
      return out
    }

    function indexLinks() {
      var links = {/* sha1: [byteOffset] */}
      for (var i = 0, j; j = buf.indexOf(0, i, 'ascii') + 1; i = j + 20) {
        var sha1 = buf.slice(j, j + 20).toString('hex')
        // Record the link from this object to its dependency
        // and the byte index of the link so we can rewrite it later
        if (!(sha1 in links)) {
          links[sha1] = true
          // add backlink to resolve function
          ;(depends[sha1] || (depends[sha1] = {}))[gitHash] = tryResolve
        }
      }
      return links
    }

    function tryResolve() {
      // console.error('try resolve', canResolve(links))
      if (!canResolve(links) || resolving) return false
      var offset = 0
      resolving = true
      waiting++
      // console.error('waiting++:', waiting, gitHash)

      // console.error('resolving tree', gitHash)

      // re-find and rewrite the links
      for (var i = 0, j; i < buf.length; i = j + 20) {
        j = buf.indexOf(0, i, 'ascii') + 1;
        var sha1 = buf.slice(j, j + 20).toString('hex')
        var hash = hashCache[sha1]
        if (!hash)
          return out.end(new Error('missing hash for ' + sha1))
        // pass through file info, null byte, and git hash
        out.push(buf.slice(i, j + 20))
        // append other hash
        out.push(new Buffer(hash))
        // remove used dependency link
        // console.error('deleting dep', !!depends[sha1][gitHash])
        // delete depends[sha1][gitHash]
      }
      out.end(true)
      return true
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
