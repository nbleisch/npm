module.exports = updateIndex

var fs = require('graceful-fs')
var assert = require('assert')
var path = require('path')
var mkdir = require('mkdirp')
var chownr = require('chownr')
var npm = require('../npm.js')
var log = require('npmlog')
var cacheFile = require('npm-cache-filename')
var getCacheStat = require('./get-stat.js')
var mapToRegistry = require('../utils/map-to-registry.js')
var jsonstream = require('JSONStream')
var writeStreamAtomic = require('fs-write-stream-atomic')
var ms = require('mississippi')
var sortedUnionStream = require('sorted-union-stream')

var MAX_SEARCH_CACHE_AGE = 60000

// Updates the search index (aka the entire metadata db in a flat file)
// Returns a stream that ends when the update is done, and returns
// cache entries as they're processed.
function updateIndex (staleness, cb) {
  assert(typeof cb === 'function', 'must pass final callback to updateIndex')

  mapToRegistry('-/all', npm.config, function (er, uri, auth) {
    if (er) return cb(er)

    var cacheBase = cacheFile(npm.config.get('cache'))(uri)
    var cachePath = path.join(cacheBase, '.cache.json')

    createEntryStream(cachePath, uri, auth, staleness, function (err, entryStream, latest) {
      if (err) return cb(err)
      log.silly('update-index', 'entry stream created')
      ms.finished(entryStream, function (err) {
        log.warn('update-index', 'entry stream finished. err:', err)
      })
      if (latest) {
        createCacheWriteStream(cachePath, latest, function (err, writeStream) {
          if (err) return cb(err)
          log.silly('update-index', 'output stream created')
          cb(null, ms.pipeline.obj(entryStream, writeStream))
        })
      } else {
        cb(null, entryStream)
      }
    })
  })
}

// Creates a stream of the latest available package metadata.
// Metadata will come from a combination of the local cache and remote data.
function createEntryStream (cachePath, uri, auth, staleness, cb) {
  createCacheEntryStream(cachePath, function (err, cacheStream, latest) {
    latest = latest || 0
    if (err) {
      log.warn('', 'Failed to read search cache. Rebuilding')
      log.silly('update-index', 'cache read error: ', err)
    }
    createEntryUpdateStream(uri, auth, staleness, latest, function (err, updateStream, latest) {
      latest = latest || 0
      if (!cacheStream && !updateStream) {
        return cb(new Error('No search sources available'))
      }
      if (err) {
        log.warn('', 'Search data request failed, search might be stale')
        log.silly('update-index', 'update request error: ', err)
      }
      if (cacheStream && updateStream) {
        cb(null, createMergedStream(cacheStream, updateStream), latest)
      } else {
        // Either one works if one or the other failed
        cb(null, cacheStream || updateStream, latest)
      }
    })
  })
}

// Merges `a` and `b` into one stream, dropping duplicates in favor of entries
// in `b`. Both input streams should already be individually sorted, and the
// returned output stream will have semantic resembling the merge step of a
// plain old merge sort.
function createMergedStream (a, b) {
  linkStreams(a, b)
  return sortedUnionStream(a, b, function (data) { return data.name })
}

// Reads the local index and returns a stream that spits out package data.
function createCacheEntryStream (cacheFile, cb) {
  log.info('update-index', 'creating entry stream from local cache')
  log.info('update-index', cacheFile)
  fs.stat(cacheFile, function (err, stat) {
    if (err) return cb(err)
    var entryStream = ms.pipeline.obj(
      fs.createReadStream(cacheFile),
      jsonstream.parse('*')
    )
    extractUpdated(entryStream, 'cached-entry-stream', cb)
  })
}

// Stream of entry updates from the server. If `latest` is not `0`, streams
// the entire metadata object from the registry.
function createEntryUpdateStream (all, auth, timeout, latest, cb) {
  log.info('update-index', 'creating remote entry stream')
  var params = {
    timeout: timeout,
    follow: true,
    staleOk: true,
    auth: auth,
    streaming: true
  }
  var partialUpdate = false
  if (Date.now() - latest > MAX_SEARCH_CACHE_AGE) {
    if (latest === 0) {
      log.warn('', 'Building the local index for the first time, please be patient')
      log.silly('update-index', 'No cached data: requesting full metadata db')
    } else {
      log.verbose('update-index', 'Cached data present with timestamp:', latest, 'requesting partial index update')
      all += '/since?stale=update_after&startkey=' + latest
      partialUpdate = true
    }
  }
  npm.registry.request(all, params, function (er, res) {
    log.silly('update-index', 'request stream opened, code:', res.statusCode)
    if (er) return cb(er)
    var entryStream = ms.pipeline.obj(
      res,
      ms.through(function (chunk, enc, cb) {
        cb(null, chunk)
      }),
      jsonstream.parse('*', function (pkg, key) {
        if (key[0] === '_updated' || key[0][0] !== '_') {
          return pkg
        }
      })
    )
    if (partialUpdate) {
      // The `/all/since` endpoint doesn't return `_updated`, so we
      // just use the request's own timestamp.
      cb(null, entryStream, Date.parse(res.headers.date))
    } else {
      extractUpdated(entryStream, 'entry-update-stream', cb)
    }
  })
}

// Both the (full) remote requests and the local index have `_updated` as their
// first returned entries. This is the "latest" unix timestamp for the metadata
// in question. This code does a bit of juggling with the data streams
// so that we can pretend that field doesn't exist, but still extract `latest`
function extractUpdated (entryStream, label, cb) {
  log.silly('update-index', 'extracting latest')
  function nope (msg) {
    return function () {
      log.warn('update-index', label, msg)
      entryStream.removeAllListeners()
      entryStream.destroy()
      cb(new Error(msg))
    }
  }
  entryStream.on('error', nope)
  entryStream.on('close', nope)
  entryStream.once('data', function (latest) {
    log.silly('update-index', 'got first stream entry for', label, latest)
    entryStream.removeListener('error', nope('Failed to read stream'))
    entryStream.removeListener('end', nope('Empty or invalid stream'))
    if (typeof latest === 'number') {
      cb(null, entryStream, latest)
    } else {
      cb(new Error('expected first entry to be _updated'))
    }
  })
}

// Creates a stream that writes input metadata to the current cache.
// Cache updates are atomic, and the stream closes when *everything* is done.
// The stream is also passthrough, so entries going through it will also
// be output from it.
function createCacheWriteStream (cacheFile, latest, cb) {
  var cacheBase = path.dirname(cacheFile)
  log.silly('update-index', 'making sure cache dir exists at', cacheBase)
  getCacheStat(function (er, st) {
    if (er) return cb(er)
    mkdir(cacheBase, function (er, made) {
      if (er) return cb(er)
      chownr(made || cacheBase, st.uid, st.gid, function (er) {
        if (er) return cb(er)
        var updatedWritten = false
        log.silly('update-index', 'creating output stream')

        var writer = writeStreamAtomic(cacheFile)
        var passThrough = ms.pipeline.obj(
          ms.through.obj(function (pkg, enc, cb) {
            if (!updatedWritten) {
              this.push(['_updated', latest])
              updatedWritten = true
            }
            cb(null, [pkg.name, pkg])
          }),
          jsonstream.stringifyObject('{', ',', '}'),
          ms.through(function (chunk, enc, cb) {
            writer.write(chunk, enc, function () {
              cb(null, chunk)
            })
          }),
          jsonstream.parse('*')
        )

        // Glue together `passThrough` and `writer` so they fail together.
        var errEmitted = false
        linkStreams(passThrough, writer, function () { errEmitted = true })
        passThrough.on('end', function () { !errEmitted && writer.end() })

        cb(null, passThrough)
      })
    })
  })
}

// Links errors between `a` and `b`, preventing cycles, and calls `cb` if
// an error happens, once per error.
function linkStreams (a, b, cb) {
  var lastError = null
  a.on('error', function (err) {
    if (err !== lastError) {
      lastError = err
      b.emit('error', err)
      cb(err)
    }
  })
  b.on('error', function (err) {
    if (err !== lastError) {
      lastError = err
      a.emit('error', err)
      cb(err)
    }
  })
}
