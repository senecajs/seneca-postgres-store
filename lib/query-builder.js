'use strict'

var RelationalStore = require('./relational-util')
var _ = require('lodash')
var OpParser = require('./operator_parser')
var mySQLStore = 'mysql-store'
var postgresStore = 'postgresql-store'
var operatorMap = {
  'or$': 'OR',
  'and$': 'AND'
}

module.exports = function (opts) {
  var fromColumn = opts.fromColumnName || _.identity
  var toColumn = opts.toColumnName || _.identity

  function parseExpression (sTypes, currentName, currentValue, params, values) {
    var result = {
      processed: true
    }

    if (currentName.indexOf('$') !== -1) {
      return result
    }

    if (currentValue === null) {
      // we can't use the equality on null because NULL != NULL
      params.push(sTypes.escape + RelationalStore.escapeStr(toColumn(currentName)) + sTypes.escape + ' IS NULL')
      return result
    }

    if (currentValue instanceof RegExp) {
      var op = (currentValue.ignoreCase) ? '~*' : '~'
      values.push(currentValue.source)
      params.push(sTypes.escape + RelationalStore.escapeStr(toColumn(currentName)) + sTypes.escape + op + RelationalStore.preparedStatements(sTypes.name, values.length))
      return result
    }

    if (_.isObject(currentValue)) {
      result.processed = false
      return result
    }
    else {
      values.push(currentValue)
      params.push(sTypes.escape + RelationalStore.escapeStr(toColumn(currentName)) + sTypes.escape + '=' + RelationalStore.preparedStatements(sTypes.name, values.length))
      return result
    }

    return result
  }

  function parseExtendedExpression (sTypes, currentName, currentValue, params, values) {
    function parseComplexSelectOperator (sTypes, currentName, currentValue, params, values) {
      var result = {}

      result.processed = _.every(currentValue, function (opVal, op) {
        if (!OpParser[op]) {
          result.err = 'This operator is not yet implemented: ' + op
          return false
        }
        var err = OpParser[op](currentName, opVal, params, values, sTypes)
        if (err) {
          result.err = err
          return false
        }

        return true
      })

      return result
    }

    var result = parseExpression(sTypes, currentName, currentValue, params, values)

    if (!result.processed && _.isObject(currentValue)) {
      result = parseComplexSelectOperator(sTypes, currentName, currentValue, params, values)
      if (result.err) {
        return result
      }
    }

    return result
  }


  function parseOperation (entp, sTypes, currentName, currentValue, params, values, operator, parseExpression) {
    if (!_.isArray(currentValue)) {
      return {err: operator + ' operator requires an array value'}
    }

    var results = []
    for (var i in currentValue) {
      var w = whereargs(entp, currentValue[i])
      var subParams = []
      for (var j in w.params) {
        if (operatorMap[w.params[j]]) {
          parseOperation(entp, sTypes, w.params[j], w.values[j], subParams, values, operatorMap[w.params[j]], parseExpression)
          results.push(subParams.join(' AND '))
          continue
        }
        var current_result = parseExpression(sTypes, w.params[j], w.values[j], subParams, values)
        if (current_result.err) {
          return current_result
        }
        results.push(subParams.join(' AND '))
      }
    }

    var resultStr = ''
    for (i in results) {
      if (resultStr.length > 0) {
        resultStr += ' ' + operator + ' '
      }
      resultStr += results[i]
    }
    params.push('(' + resultStr + ')')
  }

  function buildQueryFromExpressionCustom (entp, query_parameters, sTypes, values, parseExpression) {
    var params = []
    values = values || []

    if (!_.isEmpty(query_parameters) && query_parameters.params.length > 0) {
      for (var i in query_parameters.params) {
        var currentName = query_parameters.params[i]
        var currentValue = query_parameters.values[i]

        if (operatorMap[currentName]) {
          parseOperation(entp, sTypes, currentName, currentValue, params, values, operatorMap[currentName], parseExpression)
          continue
        }

        var result = parseExpression(sTypes, currentName, currentValue, params, values)
        if (result.err) {
          return result
        }
      }

      return {err: null, data: params.join(' AND '), values: values}
    }
    else {
      return {values: values}
    }
  }

  function buildQueryFromExpression (entp, queryParameters, sTypes, values) {
    return buildQueryFromExpressionCustom(entp, queryParameters, sTypes, values, parseExtendedExpression)
  }

  function whereargs (entp, q) {
    var w = {}

    w.params = []
    w.values = []

    var qok = RelationalStore.fixquery(entp, q)

    for (var p in qok) {
      if (qok[p] !== undefined) {
        w.params.push(toColumn(p))
        w.values.push(qok[p])
      }
    }

    return w
  }

  function fixPrepStatement (stm, sTypes) {
    if (sTypes.name === mySQLStore) {
      return stm
    }

    var index = 1
    while (stm.indexOf('?') !== -1) {
      stm = stm.replace('?', '$' + index)
      index++
    }
    return stm
  }

  function jsonSupport (sTypes) {
    return (sTypes.name === postgresStore)
  }

  function savestm (ent, sTypes) {
    var stm = {}

    var table = RelationalStore.tablename(ent)
    var entp = RelationalStore.makeentp(ent, jsonSupport(sTypes))
    var fields = _.keys(entp)

    var values = []
    var params = []

    var cnt = 0

    var escapedFields = []
    fields.forEach(function (field) {
      escapedFields.push(sTypes.escape + RelationalStore.escapeStr(toColumn(field)) + sTypes.escape)
      values.push(entp[field])
      params.push(RelationalStore.preparedStatements(sTypes.name, ++cnt))
    })

    stm.text = 'INSERT INTO ' + RelationalStore.escapeStr(table) +
      ' (' + escapedFields + ') VALUES (' + RelationalStore.escapeStr(params) + ')' +
      ' RETURNING *'

    stm.values = values

    return stm
  }

  function updatestm (ent, sTypes) {
    var stm = {}

    var table = RelationalStore.tablename(ent)
    var entp = RelationalStore.makeentp(ent, jsonSupport(sTypes))
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
        params.push(sTypes.escape + RelationalStore.escapeStr(toColumn(field)) + sTypes.escape + '=' + RelationalStore.preparedStatements(sTypes.name, ++cnt))
      }
    })

    stm.text = 'UPDATE ' + RelationalStore.escapeStr(table) + ' SET ' + params + " WHERE id='" + RelationalStore.escapeStr(ent.id) + "'"
    stm.values = values

    return stm
  }

  function schemastm(ent, _sTypes) {
    var table = RelationalStore.tablename(ent)

    var text = 
      'SELECT column_name, data_type ' +
      'FROM information_schema.columns ' +
      'WHERE table_name = ' + "'" + RelationalStore.escapeStr(table) + "'"

    return { text, values: [] }
  }

  function upsertbyidstm(ent, sTypes, schema, opts) {
    var shouldMerge = 'merge' in opts ? opts.merge : true

    var table = RelationalStore.tablename(ent)
    var entp = RelationalStore.makeentp(ent, jsonSupport(sTypes))

    if (shouldMerge) {
      var fields = fieldsRequestedForUpdate(entp)
      var values = valuesForMerge(fields, entp)

      return buildStatement(fields, values)
    }

    var fields = allEntityFields(schema)
    var values = valuesForReplacement(fields, entp)

    return buildStatement(fields, values)


    function fieldsRequestedForUpdate(entp) {
      var fields = _.keys(entp)

      var publicFields = fields.filter(function (field) {
        return field.indexOf('$') === -1
      })

      var publicDefinedFields = publicFields.filter(function (field) {
        return !_.isUndefined(entp[field])
      })

      return publicDefinedFields
    }

    function valuesForMerge(fields, entp) {
      return fields.map(function (field) {
        return entp[field]
      })
    }

    function allEntityFields(schema) {
      return schema.map(function (row) {
        return fromColumn(row.column_name)
      })
    }

    function valuesForReplacement(fields, entp) {
      return fields.map(function (field) {
        return field in entp ? entp[field] : null
      })
    }

    function escapeColumn(col) {
      return sTypes.escape + RelationalStore.escapeStr(col) + sTypes.escape
    }

    function buildStatement(fields, values) {
      var params = fields.map(function (field, i) {
        var placeholderNum = i + 1
        return RelationalStore.preparedStatements(sTypes.name, placeholderNum)
      })

      var escapedParams = params


      var paramsForUpdate = fields.map(function (_, i) {
        var key = fields[i]
        var value = params[i]

        return [key, value]
      })

      var escapedParamsForUpdate = paramsForUpdate.map(function (kv) {
        var col = kv[0]
        var escapedColumn = sTypes.escape + RelationalStore.escapeStr(col) + sTypes.escape

        var valuePlaceholder = kv[1]

        return [escapedColumn, valuePlaceholder].join('=')
      })


      var columns = fields.map(toColumn)
      var escapedColumns = columns.map(escapeColumn)


      var stm = {
        text: 'INSERT INTO ' + RelationalStore.escapeStr(table) +
          ' (' + escapedColumns.join(', ') + ') VALUES (' + escapedParams.join(', ') + ')' +
          ' ON CONFLICT (id) DO UPDATE SET ' +
          escapedParamsForUpdate.join(', ') + ' RETURNING *',

        values: values
      }


      return stm
    }
  }

  function deletestm (qent, q, sTypes) {
    var stm = {}

    var table = RelationalStore.tablename(qent)
    var entp = RelationalStore.makeentp(qent, jsonSupport(sTypes))

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

        params.push(sTypes.escape + RelationalStore.escapeStr(toColumn(param)) + sTypes.escape + '=' + RelationalStore.preparedStatements(sTypes.name, ++cnt))
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

  function deletebyidsstm (qent, ids, sTypes) {
    var table = RelationalStore.tablename(qent)

    var valuePlaceholders = ids.map(function (_, i) {
      var placeholderNum = i + 1
      return RelationalStore.preparedStatements(sTypes.name, placeholderNum)
    })

    var stm = {
      text:
        'DELETE FROM ' + RelationalStore.escapeStr(table) + (
          valuePlaceholders.length > 0
            ? ' WHERE id IN (' + valuePlaceholders.join(', ') + ')'
            : ' WHERE false'
        ),

      values: ids
    }

    return stm
  }

  function selectstmCustom (qent, q, sTypes, queryFromExpressionBuilder, done) {
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
    var entp = RelationalStore.makeentp(qent, jsonSupport(sTypes))


    var w = whereargs(entp, q)

    var response = queryFromExpressionBuilder(entp, w, sTypes)
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

  function selectstm (qent, q, sTypes, done) {
    selectstmCustom(qent, q, sTypes, buildQueryFromExpression, done)
  }

  function selectstmOr (qent, q, sTypes) {
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
    var entp = RelationalStore.makeentp(qent, jsonSupport(sTypes))

    var values = []
    var params = []

    var cnt = 0

    var w = whereargs(entp, q.ids)

    var wherestr = ''

    if (!_.isEmpty(w) && w.params.length > 0) {
      w.params.forEach(function (param) {
        params.push(sTypes.escape + RelationalStore.escapeStr(toColumn('id')) + sTypes.escape + '=' + RelationalStore.preparedStatements(sTypes.name, ++cnt))
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

    var what = '*'
    if (specialOpsVal['fields$'] && _.isArray(specialOpsVal['fields$']) && specialOpsVal['fields$'].length > 0) {
      what = ' ' + specialOpsVal['fields$'].join(', ')
      what += ', id '
    }

    stm.text = 'SELECT ' + what + ' FROM ' + RelationalStore.escapeStr(table) + wherestr + RelationalStore.escapeStr(metastr)
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
      mq.params.push('ORDER BY ' + toColumn(sf) + ' ' + sd)
    }

    if (q.limit$) {
      mq.params.push('LIMIT ' + q.limit$)
    }

    if (typeof q.skip$ === 'number' && q.skip$ >= 0) {
      mq.params.push('OFFSET ' + q.skip$)
    }

    return mq
  }

  function buildSelectStatementCustom (qent, q, sTypes, selectstm, selectstmOr, done) {
    if (_.isString(q.native$)) {
      return done(null, q.native$)
    }

    if (_.isArray(q.native$)) {
      // first element in array should be query, the other being values
      //
      if (q.native$.length === 0) {
        return done(new Error('Invalid query'))
      }
      var query = {}
      query.text = fixPrepStatement(q.native$[0], sTypes)
      query.values = _.clone(q.native$)
      query.values.splice(0, 1)

      return done(null, query)
    }

    if (q.ids) {
      return done(null, selectstmOr(qent, q, sTypes))
    }

    if (_.isArray(q)) {
      return done(null, selectstmOr(qent, { ids: q }, sTypes))
    }

    return selectstm(qent, q, sTypes, done)
  }

  function buildSelectStatement (qent, q, sTypes, done) {
    buildSelectStatementCustom(qent, q, sTypes, selectstm, selectstmOr, done)
  }

  return {
    buildQueryFromExpression: buildQueryFromExpression,
    updatestm: updatestm,
    schemastm: schemastm,
    upsertbyidstm: upsertbyidstm,
    deletestm: deletestm,
    deletebyidsstm: deletebyidsstm,
    selectstm: selectstm,
    selectstmCustom: selectstmCustom,
    savestm: savestm,
    fixPrepStatement: fixPrepStatement,
    selectstmOr: selectstmOr,
    jsonSupport: jsonSupport,
    whereargs: whereargs,
    metaquery: metaquery,
    buildQueryFromExpressionCustom: buildQueryFromExpressionCustom,
    parseExpression: parseExpression,
    makeent: RelationalStore.makeent,
    fromColumnName: fromColumn,
    toColumnName: toColumn,
    buildSelectStatement: buildSelectStatement,
    buildSelectStatementCustom: buildSelectStatementCustom
  }
}
