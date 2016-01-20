'use strict'

var RelationalStore = require('./relational-util')
var _ = require('lodash')
var OpParser = require('./operator_parser')

var buildQueryFromExpression = function (entp, query_parameters, values) {
  var params = []
  values = values || []

  if (!_.isEmpty(query_parameters) && query_parameters.params.length > 0) {
    for (var i in query_parameters.params) {
      var current_name = query_parameters.params[i]
      var current_value = query_parameters.values[i]

      var result = parseExpression(current_name, current_value)
      if (result.err) {
        return result
      }
    }

    return {err: null, data: params.join(' AND '), values: values}
  }
  else {
    return {values: values}
  }

  function parseOr (current_name, current_value) {
    if (!_.isArray(current_value)) {
      return {err: 'or$ operator requires an array value'}
    }

    var results = []
    for (var i in current_value) {
      var w = whereargs(entp, current_value[i])
      var current_result = buildQueryFromExpression(entp, w, values)
      values = current_result.values
      results.push(current_result)
    }

    var resultStr = ''
    for (i in results) {
      if (resultStr.length > 0) {
        resultStr += ' OR '
      }
      resultStr += results[i].data
    }
    console.log('(' + resultStr + ')')
    params.push('(' + resultStr + ')')
  }

  function parseAnd (current_name, current_value) {
    if (!_.isArray(current_value)) {
      return {err: 'and$ operator requires an array value'}
    }

    var results = []
    for (var i in current_value) {
      var w = whereargs(entp, current_value[i])
      var current_result = buildQueryFromExpression(entp, w, values)
      values = current_result.values
      results.push(current_result)
    }

    var resultStr = ''
    for (i in results) {
      if (resultStr.length > 0) {
        resultStr += ' AND '
      }
      resultStr += results[i].data
    }
    console.log('(' + resultStr + ')')
    params.push('(' + resultStr + ')')
  }

  function parseExpression (current_name, current_value) {
    if (current_name === 'or$') {
      parseOr(current_name, current_value)
    }
    else if (current_name === 'and$') {
      parseAnd(current_name, current_value)
    }
    else {
      if (current_name.indexOf('$') !== -1) {
        return {}
      }

      if (current_value === null) {
        // we can't use the equality on null because NULL != NULL
        params.push('"' + RelationalStore.escapeStr(RelationalStore.camelToSnakeCase(current_name)) + '" IS NULL')
      }
      else if (current_value instanceof RegExp) {
        var op = (current_value.ignoreCase) ? '~*' : '~'
        values.push(current_value.source)
        params.push('"' + RelationalStore.escapeStr(RelationalStore.camelToSnakeCase(current_name)) + '"' + op + '$' + values.length)
      }
      else if (_.isObject(current_value)) {
        var result = parseComplexSelectOperator(current_name, current_value, params)
        if (result.err) {
          return result
        }
      }
      else {
        values.push(current_value)
        params.push('"' + RelationalStore.escapeStr(RelationalStore.camelToSnakeCase(current_name)) + '"' + '=' + '$' + values.length)
      }
    }
    return {}
  }

  function parseComplexSelectOperator (current_name, current_value, params) {
    for (var op in current_value) {
      var op_val = current_value[op]
      if (!OpParser[op]) {
        return {err: 'This operator is not yet implemented: ' + op}
      }
      var err = OpParser[op](current_name, op_val, params, values)
      if (err) {
        return {err: err}
      }
    }
    return {}
  }
}

function whereargs (entp, q) {
  var w = {}

  w.params = []
  w.values = []

  var qok = RelationalStore.fixquery(entp, q)

  for (var p in qok) {
    if (qok[p] !== undefined) {
      w.params.push(RelationalStore.camelToSnakeCase(p))
      w.values.push(qok[p])
    }
  }

  return w
}


function fixPrepStatement (stm) {
  var index = 1
  while (stm.indexOf('?') !== -1) {
    stm = stm.replace('?', '$' + index)
    index++
  }
  return stm
}

function savestm (ent) {
  var stm = {}

  var table = RelationalStore.tablename(ent)
  var entp = RelationalStore.makeentp(ent)
  var fields = _.keys(entp)

  var values = []
  var params = []

  var cnt = 0

  var escapedFields = []
  fields.forEach(function (field) {
    escapedFields.push('"' + RelationalStore.escapeStr(RelationalStore.camelToSnakeCase(field)) + '"')
    values.push(entp[field])
    params.push('$' + (++cnt))
  })

  stm.text = 'INSERT INTO ' + RelationalStore.escapeStr(table) + ' (' + escapedFields + ') values (' + RelationalStore.escapeStr(params) + ')'
  stm.values = values

  return stm
}

function updatestm (ent) {
  var stm = {}

  var table = RelationalStore.tablename(ent)
  var entp = RelationalStore.makeentp(ent)
  var fields = _.keys(entp)

  var values = []
  var params = []
  var cnt = 0

  fields.forEach(function (field) {
    if (field.indexOf('$') !== -1) {
      return
    }

    if (!_.isUndefined(entp[field])) {
      values.push(entp[field])
      params.push('"' + RelationalStore.escapeStr(RelationalStore.camelToSnakeCase(field)) + '"=$' + (++cnt))
    }
  })

  stm.text = 'UPDATE ' + RelationalStore.escapeStr(table) + ' SET ' + params + " WHERE id='" + RelationalStore.escapeStr(ent.id) + "'"
  stm.values = values

  return stm
}

function deletestm (qent, q) {
  var stm = {}

  var table = RelationalStore.tablename(qent)
  var entp = RelationalStore.makeentp(qent)

  var values = []
  var params = []

  var cnt = 0

  var w = whereargs(entp, q)

  var wherestr = ''

  if (!_.isEmpty(w) && w.params.length > 0) {
    for (var i in w.params) {
      var param = w.params[i]
      var val = w.values[i]

      if (param.indexOf('$') !== -1) {
        continue
      }

      params.push('"' + RelationalStore.escapeStr(RelationalStore.camelToSnakeCase(param)) + '"=$' + (++cnt))
      values.push(RelationalStore.escapeStr(val))
    }

    if (params.length > 0) {
      wherestr = ' WHERE ' + params.join(' AND ')
    }
    else {
      wherestr = ' '
    }
  }

  stm.text = 'DELETE FROM ' + RelationalStore.escapeStr(table) + wherestr
  stm.values = values

  return stm
}

function filterStatement (qent, q) {
  var stm = {}

  var table = RelationalStore.tablename(qent)
  var entp = RelationalStore.makeentp(qent)

  var values = []
  var params = []

  var cnt = 0

  var w = whereargs(entp, q)

  var wherestr = ''

  if (!_.isEmpty(w) && w.params.length > 0) {
    w.params.forEach(function (param) {
      params.push('"' + RelationalStore.escapeStr(RelationalStore.camelToSnakeCase(param)) + '"=$' + (++cnt))
    })

    w.values.forEach(function (value) {
      values.push(value)
    })

    wherestr = ' WHERE ' + params.join(' AND ')
  }

  var mq = metaquery(qent, q)

  var metastr = ' ' + mq.params.join(' ')

  var filterParams = q.distinct$ || q.fields$

  var selectColumns = []
  if (filterParams && !_.isString(filterParams) && _.isArray(filterParams)) {
    selectColumns = filterParams
  }
  if (selectColumns.length === 0) {
    selectColumns.push('*')
  }

  var select = 'SELECT ' + (q.distinct$ ? 'DISTINCT ' : '')
  stm.text = select + RelationalStore.escapeStr(selectColumns.join(',')) + ' FROM ' + RelationalStore.escapeStr(table) + wherestr + RelationalStore.escapeStr(metastr)
  stm.values = values

  return stm
}

function selectstm (qent, q, done) {
  var specialOps = ['fields$']
  var specialOpsVal = {}

  var stm = {}

  for (var i in specialOps) {
    if (q[specialOps[i]]) {
      specialOpsVal[specialOps[i]] = q[specialOps[i]]
      delete q[specialOps[i]]
    }
  }

  var table = RelationalStore.tablename(qent)
  var entp = RelationalStore.makeentp(qent)


  var w = whereargs(entp, q)

  var response = buildQueryFromExpression(entp, w)
  if (response.err) {
    return done(response.err)
  }

  var wherestr = response.data

  var values = response.values

  var mq = metaquery(qent, q)

  var metastr = ' ' + mq.params.join(' ')

  var what = '*'
  if (specialOpsVal['fields$'] && _.isArray(specialOpsVal['fields$']) && specialOpsVal['fields$'].length > 0) {
    what = ' ' + specialOpsVal['fields$'].join(', ')
    what += ', id '
  }

  stm.text = 'SELECT ' + what + ' FROM ' + RelationalStore.escapeStr(table) + (wherestr ? ' WHERE ' + wherestr : '') + RelationalStore.escapeStr(metastr)
  stm.values = values

  done(null, stm)
}

function selectstmOr (qent, q) {
  var stm = {}

  var table = RelationalStore.tablename(qent)
  var entp = RelationalStore.makeentp(qent)

  var values = []
  var params = []

  var cnt = 0

  var w = whereargs(entp, q.ids)

  var wherestr = ''

  if (!_.isEmpty(w) && w.params.length > 0) {
    w.params.forEach(function (param) {
      params.push('"' + RelationalStore.escapeStr(RelationalStore.camelToSnakeCase('id')) + '"=$' + (++cnt))
    })

    w.values.forEach(function (value) {
      values.push(value)
    })

    wherestr = ' WHERE ' + params.join(' OR ')
  }

  // This is required to set the limit$ to be the length of the 'ids' array, so that in situations
  // when it's not set in the query(q) it won't be applied the default limit$ of 20 records
  if (!q.limit$) {
    q.limit$ = q.ids.length
  }

  var mq = metaquery(qent, q)

  var metastr = ' ' + mq.params.join(' ')

  stm.text = 'SELECT * FROM ' + RelationalStore.escapeStr(table) + wherestr + RelationalStore.escapeStr(metastr)
  stm.values = values

  return stm
}

function metaquery (qent, q) {
  var mq = {}

  mq.params = []
  mq.values = []

  if (q.sort$) {
    for (var sf in q.sort$) break
    var sd = q.sort$[sf] > 0 ? 'ASC' : 'DESC'
    mq.params.push('ORDER BY ' + RelationalStore.camelToSnakeCase(sf) + ' ' + sd)
  }

  if (q.limit$) {
    mq.params.push('LIMIT ' + q.limit$)
  }

  if (q.skip$) {
    mq.params.push('OFFSET ' + q.skip$)
  }

  return mq
}

module.exports.buildQueryFromExpression = buildQueryFromExpression
module.exports.updatestm = updatestm
module.exports.deletestm = deletestm
module.exports.selectstm = selectstm
module.exports.savestm = savestm
module.exports.fixPrepStatement = fixPrepStatement
module.exports.selectstmOr = selectstmOr
module.exports.filterStatement = filterStatement
