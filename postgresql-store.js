'use strict'

var _ = require('lodash')
var Pg = require('pg')
var Uuid = require('node-uuid')
var name = 'postgresql-store'
var actionRole = 'sql'

var RelationalStore = require('./lib/relational-util')

var MIN_WAIT = 16
var MAX_WAIT = 5000

module.exports = function (opts) {
  var seneca = this

  opts.minwait = opts.minwait || MIN_WAIT
  opts.maxwait = opts.maxwait || MAX_WAIT

  var ColumnNameParsing = {
    fromColumnName: opts.fromColumnName || _.identity,
    toColumnName: opts.toColumnName || _.identity
  }
  var QueryBuilder = require('./lib/query-builder')(ColumnNameParsing)


  var minwait
  var internals = {}

  function error (query, args, err/*, next*/) {
    if (err) {
      var errorDetails = {
        message: err.message,
        err: err,
        stack: err.stack,
        query: query
      }
      seneca.log.error('Query Failed', JSON.stringify(errorDetails, null, 1))
      // next ({code: 'entity/error', store: name})

      if ('ECONNREFUSED' === err.code || 'notConnected' === err.message || 'Error: no open connections' === err) {
        minwait = opts.minwait
        if (minwait) {
          reconnect(args)
        }
      }

      return true
    }

    return false
  }

  function reconnect (args) {
    seneca.log.debug('attempting db reconnect')

    configure(opts, function (err) {
      if (err) {
        seneca.log.debug('db reconnect (wait ' + opts.minwait + 'ms) failed: ' + err)
        minwait = Math.min(2 * minwait, opts.maxwait)
        setTimeout(function () {
          reconnect(args)
        }, minwait)
      }
      else {
        minwait = opts.minwait
        seneca.log.debug('reconnect ok')
      }
    })
  }

  var pgConf

  function configure (spec, done) {
    pgConf = 'string' === typeof (spec) ? null : spec

    if (!pgConf) {
      pgConf = {}

      var urlM = /^postgres:\/\/((.*?):(.*?)@)?(.*?)(:?(\d+))?\/(.*?)$/.exec(spec)
      pgConf.name = urlM[7]
      pgConf.port = urlM[6]
      pgConf.host = urlM[4]
      pgConf.username = urlM[2]
      pgConf.password = urlM[3]

      pgConf.port = pgConf.port ? parseInt(pgConf.port, 10) : null
    }

    // pg conf properties
    pgConf.user = pgConf.username
    pgConf.database = pgConf.name

    pgConf.host = pgConf.host || pgConf.server
    pgConf.username = pgConf.username || pgConf.user
    pgConf.password = pgConf.password || pgConf.pass

    setImmediate(function () {
      return done(undefined)
    })
  }

  function execQuery (query, done) {
    Pg.connect(pgConf, function (err, client, releaseConnection) {
      if (err) {
        seneca.log.error('Connection error', err)
        return done(err, undefined)
      }
      else {
        if (!query) {
          err = new Error('Query cannot be empty')
          seneca.log.error('An empty query is not a valid query', err)
          releaseConnection()
          return done(err, undefined)
        }

        client.query(query, function (err, res) {
          releaseConnection()
          return done(err, res)
        })
      }
    })
  }


  var store = {
    name: name,

    close: function (args, done) {
      Pg.end()
      setImmediate(done)
    },

    save: function (args, done) {
      var seneca = this

      var ent = args.ent
      var sTypes = specificTypes(name)

      var q = args.q
      var autoIncrement = q.auto_increment$ || false

      return getSchema(ent, sTypes, function (err, schema) {
        if (err) {
          seneca.log.error('save', 'Error while pulling the schema:', err)
          return done(err)
        }


        if (isUpdate(ent)) {
          return updateEnt(ent, sTypes, schema, opts, function (err, res) {
            if (err) {
              seneca.log.error('save/update', 'Error while updating the entity:', err)
              return done(err)
            }

            var updatedAnything = res.rowCount > 0

            if (!updatedAnything) {
              return insertEnt(ent, sTypes, function (err, res) {
                if (err) {
                  seneca.log.error('save/insert', 'Error while inserting the entity:', err)
                  return done(err)
                }

                seneca.log.debug('save/insert', res)

                return done(null, res)
              })
            }

            return findEnt(ent, { id: ent.id }, sTypes, function (err, res) {
              if (err) {
                seneca.log.error('save/update', 'Error while fetching the updated entity:', err)
                return done(err)
              }

              seneca.log.debug('save/update', res)

              return done(null, res)
            })
          })
        }


        return generateId(seneca, name, function (err, generatedId) {
          if (err) {
            seneca.log.error('save/insert', 'Error while generating an id for the entity:', err)
            return done(err)
          }


          var newId = null == ent.id$
            ? generatedId
            : ent.id$


          var newEnt = ent.clone$()

          if (!autoIncrement) {
            newEnt.id = newId
          }


          if (isUpsert(ent, q)) {
            return upsertEnt(newEnt, q, sTypes, function (err, res) {
              if (err) {
                seneca.log.error('save/upsert', 'Error while inserting the entity:', err)
                return done(err)
              }

              seneca.log.debug('save/upsert', res)

              return done(null, res)
            })
          }


          return insertEnt(newEnt, sTypes, function (err, res) {
            if (err) {
              seneca.log.error('save/insert', 'Error while inserting the entity:', err)
              return done(err)
            }

            seneca.log.debug('save/insert', res)

            return done(null, res)
          })
        })
      })

      function isUpsert(ent, q) {
        return !isUpdate(ent) &&
          Array.isArray(q.upsert$) &&
          internals.cleanArray(q.upsert$).length > 0
      }


      function isUpdate(ent) {
        return null != ent.id
      }
    },

    load: function (args, done) {
      var seneca = this

      var qent = args.qent
      var q = args.q
      var sTypes = specificTypes(args.target)

      return findEnt(qent, q, sTypes, function (err, res) {
        if (err) {
          seneca.log.error('load', 'Error while fetching the entity:', err)
          return done(err)
        }

        seneca.log.debug('load', res)

        return done(null, res)
      })
    },

    list: function (args, done) {
      var seneca = this

      var qent = args.qent
      var q = args.q
      var sTypes = specificTypes(args.target)

      return listEnts(qent, q, sTypes, function (err, res) {
        if (err) {
          seneca.log.error('list', 'Error while listing the entities:', err)
          return done(err)
        }

        seneca.log.debug('list', q, res.length)

        return done(null, res)
      })
    },

    remove: function (args, done) {
      var seneca = this

      var qent = args.qent
      var q = args.q
      var sTypes = specificTypes(args.target)

      return removeEnt(qent, q, sTypes, function (err, res) {
        if (err) {
          seneca.log.error('remove', 'Error while removing the entity/entities:', err)
          return done(err)
        }

        seneca.log.debug('remove', q)

        return done(null, res)
      })
    },

    native: function (args, done) {
      Pg.connect(pgConf, done)
    }
  }

  internals.transformDBRowToJSObject = function (row) {
    var obj = {}
    for (var attr in row) {
      if (row.hasOwnProperty(attr)) {
        obj[ColumnNameParsing.fromColumnName(attr)] = row[attr]
      }
    }
    return obj
  }

  internals.cleanArray = function (ary) {
    var isPublicProp = (p) => !p.includes('$')
    return ary.filter(isPublicProp)
  }


  var meta = seneca.store.init(seneca, opts, store)

  seneca.add({ init: store.name, tag: meta.tag }, function (args, cb) {
    configure(opts, function (err) {
      cb(err)
    })
  })

  seneca.add({role: actionRole, hook: 'generate_id', target: store.name}, function (args, done) {
    return done(null, {id: Uuid()})
  })

  return {name: store.name, tag: meta.tag}


  function buildLoadStm(ent, q, sTypes) {
    var loadQ = _.clone(q)
    loadQ.limit$ = 1

    return QueryBuilder.selectstm(ent, loadQ, sTypes)
  }

  function buildListStm(ent, q, sTypes) {
    var cleanQ = _.clone(q)
    stripInvalidLimitInPlace(cleanQ)
    stripInvalidLimitInPlace(cleanQ)

    return QueryBuilder.buildSelectStatement(ent, cleanQ, sTypes)
  }

  function buildRemoveStm(ent, q, sTypes) {
    var cleanQ = _.clone(q)
    stripInvalidLimitInPlace(cleanQ)
    stripInvalidSkipInPlace(cleanQ)

    return QueryBuilder.deletestm(ent, cleanQ, sTypes)
  }

  function stripInvalidLimitInPlace(q) {
    if (Array.isArray(q)) {
      return
    }

    if (!(typeof q.limit$ === 'number' && q.limit$ >= 0)) {
      delete q.limit$
    }
  }

  function stripInvalidSkipInPlace(q) {
    if (Array.isArray(q)) {
      return
    }

    if (!(typeof q.skip$ === 'number' && q.skip$ >= 0)) {
      delete q.skip$
    }
  }

  function getSchema(ent, sTypes, done) {
    var query = QueryBuilder.schemastm(ent, sTypes)

    return execQuery(query, function (err, res) {
      if (err) {
        return done(err)
      }

      var schema = res.rows

      return done(null, schema)
    })
  }

  function specificTypes(storeName) {
    var sTypes = {
      escape: '"',
      prepared: '$'
    }
    sTypes.name = storeName

    if (storeName === 'mysql-store') {
      sTypes.escape = '`'
      sTypes.prepared = '?'
    }

    return sTypes
  }

  function shouldMerge(ent, plugin_opts) {
    if ('merge$' in ent) {
      return Boolean(ent.merge$)
    }

    if (plugin_opts && ('merge' in plugin_opts)) {
      return Boolean(plugin_opts.merge)
    }

    return true
  }

  function generateId(seneca, target, done) {
    return seneca.act({ role: actionRole, hook: 'generate_id', target: target }, function (err, res) {
      if (err) {
        return done(err)
      }

      var newId = res.id

      return done(null, newId)
    })
  }

  function insertEnt(ent, sTypes, done) {
    var query = QueryBuilder.savestm(ent, sTypes)

    return execQuery(query, function (err, res) {
      if (err) {
        return done(err)
      }

      if (res.rows && res.rows.length > 0) {
        // NOTE: res.rows should always be an array of length === 1,
        // however we want to play it safe here, and not crash the client
        // if something goes awry.
        //
        return done(null, makeEntOfRow(res.rows[0], ent))
      }

      return done(null, null)
    })
  }

  function findEnt(ent, q, sTypes, done) {
    try {
      var query = buildLoadStm(ent, q, sTypes)

      return execQuery(query, function (err, res) {
        if (err) {
          return done(err)
        }

        if (res.rows && res.rows.length > 0) {
          return done(null, makeEntOfRow(res.rows[0], ent))
        }

        return done(null, null)
      })
    } catch (err) {
      return done(err)
    }
  }

  function listEnts(ent, q, sTypes, done) {
    try {
      var query = buildListStm(ent, q, sTypes)

      return execQuery(query, function (err, res) {
        if (err) {
          return done(err)
        }

        var list = res.rows.map(function (row) {
          return makeEntOfRow(row, ent)
        })

        return done(null, list)
      })
    } catch (err) {
      return done(err)
    }
  }

  function upsertEnt(ent, q, sTypes, done) {
    try {
      var upsertFields = internals.cleanArray(q.upsert$)
      var query = QueryBuilder.upsertstm(ent, upsertFields, sTypes)

      return execQuery(query, function (err, res) {
        if (err) {
          return done(err)
        }

        if (res.rows && res.rows.length > 0) {
          // NOTE: res.rows should always be an array of length === 1,
          // however we want to play it safe here, and not crash the client
          // if something goes awry.
          //
          return done(null, makeEntOfRow(res.rows[0], ent))
        }

        return done(null, null)
      })
    } catch (err) {
      return done(err)
    }
  }



  function updateEnt(ent, sTypes, schema, opts, done) {
    try {
      var merge = shouldMerge(ent, opts)
      var query = QueryBuilder.updatestm(ent, sTypes, schema, { merge })

      return execQuery(query, done)
    } catch (err) {
      return done(err)
    }
  }

  function removeEnt(ent, q, sTypes, done) {
    try {
      var delQuery = buildRemoveStm(ent, q, sTypes)

      return execQuery(delQuery, function (err, res) {
        if (err) {
          return done(err)
        }

        var shouldLoad = !q.all$ && q.load$

        if (shouldLoad && res.rows.length > 0) {
          return done(null, makeEntOfRow(res.rows[0], ent))
        }

        return done(null, null)
      })
    } catch (err) {
      return done(err)
    }
  }

  function makeEntOfRow(row, baseEnt) {
    var attrs = internals.transformDBRowToJSObject(row)
    var newEnt = RelationalStore.makeent(baseEnt, attrs)

    return newEnt
  }
}

