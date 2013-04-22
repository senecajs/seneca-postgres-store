/* Copyright (c) 2012-2013 Marian Radulescu */
"use strict";

var _ = require('underscore');
var pg = require('pg');
var util = require('util')
var uuid = require('node-uuid');

var relationalstore = require('./relational-util')

var name = 'postgres-store';

var MIN_WAIT = 16
var MAX_WAIT = 5000

module.exports = function (seneca, opts, cb) {

  var desc;

  opts.minwait = opts.minwait || MIN_WAIT
  opts.maxwait = opts.maxwait || MAX_WAIT

  var minwait
  var dbinst = null
  var specifications = null


  function error(args, err, cb) {
    if (err) {
      seneca.log.debug('error: ' + err)
      seneca.fail({code: 'entity/error', store: name}, cb)

      if ('ECONNREFUSED' == err.code || 'notConnected' == err.message || 'Error: no open connections' == err) {
        if (minwait = opts.minwait) {
          reconnect(args)
        }
      }

      return true
    }

    return false
  }


  function reconnect(args) {
    seneca.log.debug('attempting db reconnect')

    configure(specifications, function (err) {
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


  function configure(spec, cb) {
    specifications = spec

    var conf = 'string' === typeof(spec) ? null : spec

    if (!conf) {
      conf = {}

      var urlM = /^postgres:\/\/((.*?):(.*?)@)?(.*?)(:?(\d+))?\/(.*?)$/.exec(spec);
      conf.name = urlM[7]
      conf.port = urlM[6]
      conf.host = urlM[4]
      conf.username = urlM[2]
      conf.password = urlM[3]

      conf.port = conf.port ? parseInt(conf.port, 10) : null
    }

    // pg conf properties
    conf.user = conf.username
    conf.database = conf.name

    conf.host = conf.host || conf.server
    conf.username = conf.username || conf.user
    conf.password = conf.password || conf.pass

    dbinst = new pg.Client(conf)

    dbinst.connect(function (err) {
      if (err) {
         console.log('Connection error',err)
      }
      cb(err)
    });

  }


  var store = {

    name: name,

    close: function (cb) {
      if (dbinst) {
        dbinst.end(cb)
      }
    },


    save: function (args, cb) {
      var ent = args.ent

      var update = !!ent.id;

      if (update) {
        var query = updatestm(ent)

        dbinst.query(query, function (err, res) {
          if (!error(args, err, cb)) {
            seneca.log(args.tag$, 'update', ent)
            cb(null, ent)
          }
          else {
            seneca.fail({code: 'update', tag: args.tag$, store: self.name, query: query, error: err}, cb)
          }
        })
      }
      else {
        ent.id = uuid()

        var query = savestm(ent)

        dbinst.query(query, function (err, res) {
          if (!error(args, err, cb)) {
            seneca.log(args.tag$, 'save', ent)
            cb(null, ent)
          }
          else {
            seneca.fail({code: 'save', tag: args.tag$, store: self.name, query: query, error: err}, cb)
          }
        })
      }
    },


    load: function (args, cb) {
      var qent = args.qent
      var q = args.q

      var query = selectstm(qent, q)

      dbinst.query(query, function (err, res) {
        if (!error(args, err, cb)) {
          var ent = relationalstore.makeent(qent, res.rows[0])
          seneca.log(args.tag$, 'load', ent)
          cb(null, ent)
        }
        else {
          seneca.fail({code: 'load', tag: args.tag$, store: self.name, query: query, error: err}, cb)
        }
      })
    },


    list: function (args, cb) {
      var qent = args.qent
      var q = args.q

      var list = []

      var query = selectstm(qent, q)

      var query = dbinst.query(query, function (err, res) {
        if (!error(args, err, cb)) {
          res.rows.forEach(function (row) {
            var ent = relationalstore.makeent(qent, row)
            list.push(ent)
          })
          seneca.log(args.tag$, 'list', list.length, list[0])
          cb(null, list)
        }
        else {
          seneca.fail({code: 'list', tag: args.tag$, store: self.name, query: query, error: err}, cb)
        }
      })
    },


    remove: function (args, cb) {
      var qent = args.qent
      var q = args.q

      if (q.all$) {
        var query = deletestm(qent, q)

        dbinst.query(query, function (err, res) {
          if (!error(args, err, cb)) {
            seneca.log(args.tag$, 'remove', res.rowCount)
            cb(null, res.rowCount)
          }
          else {
            seneca.fail({code: 'remove', tag: args.tag$, store: self.name, query: query, error: err}, cb)
          }
        })
      }
      else {
        dbinst.query(selectstm(qent, q), function (err, res) {
          if (!error(args, err, cb)) {
            var entp = res.rows[0]
            var query = deletestm(qent, entp)

            dbinst.query(query, function (err, res) {
              if (!error(args, err, cb)) {
                seneca.log(args.tag$, 'remove', res.rowCount)
                cb(null, res.rowCount)
              }
              else {
                seneca.fail({code: 'remove', tag: args.tag$, store: self.name, query: query, error: err}, cb)
              }
            })
          }
        })
      }
    },


    native: function (args, done) {
//      dbinst.collection('seneca', function(err,coll){
//        if( !error(args,err,cb) ) {
//          coll.findOne({},{},function(err,entp){
//            if( !error(args,err,cb) ) {
//              done(null,dbinst)
//            }else{
//              done(err)
//            }
//          })
//        }else{
//          done(err)
//        }
//      })
    }

  }


  var savestm = function (ent) {
    var stm = {}

    var table = relationalstore.tablename(ent)
    var entp = relationalstore.makeentp(ent)
    var fields = _.keys(entp)

    var values = []
    var params = []

    var cnt = 0

    fields.forEach(function (field) {
      values.push(entp[field])
      params.push('$' + ++cnt)
    })

    stm.text = 'INSERT INTO ' + table + ' (' + fields + ') values (' + params + ')'
    stm.values = values

    return stm
  }


  var updatestm = function (ent) {
    var stm = {}

    var table = relationalstore.tablename(ent)
    var fields = ent.fields$()
    var entp = relationalstore.makeentp(ent)

    var values = []
    var params = []
    var cnt = 0

    fields.forEach(function (field) {
      if (!(_.isUndefined(ent[field]) || _.isNull(ent[field]))) {
        values.push(entp[field])
        params.push(field + '=$' + ++cnt)
      }
    })

    stm.text = "UPDATE " + table + " SET " + params + " WHERE id='" + ent.id + "'"
    stm.values = values

    return stm
  }


  var deletestm = function (qent, q) {
    var stm = {}

    var table = relationalstore.tablename(qent)
    var entp = relationalstore.makeentp(qent)

    var values = []
    var params = []

    var cnt = 0

    var w = whereargs(entp, q)

    var wherestr = ''

    if (!_.isEmpty(w) && w.params.length > 0) {
      w.params.forEach(function (param) {
        params.push(param + '=$' + ++cnt)
      })

      if (!_.isEmpty(w.values)) {
        w.values.forEach(function (val) {
          values.push(val)
        })
      }

      wherestr = " WHERE " + params.join(' AND ')
    }

    stm.text = "DELETE FROM " + table + wherestr
    stm.values = values

    return stm
  }


  var selectstm = function (qent, q) {
    var stm = {}

    var table = relationalstore.tablename(qent)
    var entp = relationalstore.makeentp(qent)

    var values = []
    var params = []

    var cnt = 0

    var w = whereargs(entp, q)

    var wherestr = ''

    if (!_.isEmpty(w) && w.params.length > 0) {
      w.params.forEach(function (param) {
        params.push(param + '=$' + ++cnt)
      })

      w.values.forEach(function (value) {
        values.push(value)
      })

      wherestr = " WHERE " + params.join(' AND ')
    }

    var mq = metaquery(qent, q)

    var metastr = ' ' + mq['params'].join(' ')

    stm.text = "SELECT * FROM " + table + wherestr + metastr
    stm.values = values

    return stm
  }


  var whereargs = function (entp, q) {
    var w = {}

    w.params = []
    w.values = []

    var qok = relationalstore.fixquery(entp, q)

    for (var p in qok) {
      if (qok[p]) {
        w.params.push(p)
        w.values.push(qok[p])
      }
    }

    return w
  }


  var metaquery = function (qent, q) {
    var mq = {}

    mq.params = []
    mq.values = []

    if (q.sort$) {
      for (var sf in q.sort$) break;
      var sd = q.sort$[sf] < 0 ? 'ASC' : 'DESC'
      mq.params.push('ORDER BY ' + sf + ' ' + sd)
    }

    if (q.limit$) {
      mq.params.push('LIMIT ' + q.limit$)
    }

    return mq
  }


  seneca.store.init(seneca, opts, store, function (err, tag, description) {
    if (err) return cb(err);

    console.log(util.inspect(opts, false, null, false))
    console.log(opts.toString())
    desc = description

    configure(opts, function (err) {
      if (err) {
        return seneca.fail({code: 'entity/configure', store: store.name, error: err}, cb)
      }
      else cb(null, {name: store.name, tag: tag});
    })
  })

}

