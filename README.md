# git-rehash

Rewrite git objects to map them between hash spaces.

git objects are identified by a SHA1 hash of their type, length, and content
[[1] [masak]]. By mapping the objects into a different hash space, we can store
git objects in other content-addressable file systems and sync them with git's
object storage.

This module rewrites git objects (except blobs) to allow for moving them
between hash spaces while preserving hash links.

[masak]: https://gist.github.com/masak/2415865

## Example

```js
var rehash = require('git-rehash')

```

## API

This module uses [pull-streams](https://github.com/dominictarr/pull-stream).

git objects are represented by an object with the following properties:

  - `type`: the type of the object, one of
    `["tag", "commit", "tree", "blob"]`
  - `length`: the size in bytes of the object
  - `read`: readable stream of the object's data. This has to be
      drained before the git object stream can read the next object.

#### `rehash.fromGit(algorithm, lookup(gitHash, cb(err, hash))`

Create a through stream for git objects that rewrites their hashes to use
`algorithm`.

- `algorithm`: a string that may be passed to `crypto.createHash`,
  e.g. `"sha256"`
- `lookup`: a function for looking up the `algorithm`-hash of a git object,
  given the objects's git hash. This is needed for rewriting links to objects
  that are not present in the stream that `rehash` is processing.

#### `rehash.toGit(algorithm)`

Create a through stream for git objects that rewrites their hashes from
`algorithm` back to git's native hashing algorithm.

- `algorithm`: a string that may be passed to `crypto.createHash`,
  e.g. `"sha256"`

A lookup function is not needed here because the original git hash of each
object is included in the rewritten object.

## License

Fair
