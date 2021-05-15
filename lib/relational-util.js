/* Base class for relational databases */
'use strict'

var _ = require('lodash')
var SENECA_TYPE_COLUMN = 'seneca'

var OBJECT_TYPE = 'o'
var ARRAY_TYPE = 'a'
var DATE_TYPE = 'd'

var mySQLStore = 'mysql-store'
var postgresStore = 'postgresql-store'

module.exports.mySQLStore = mySQLStore
module.exports.postgresStore = postgresStore

module.exports.preparedStatements = function (dbType, paramCount) {
  if (dbType === mySQLStore) {
    return '?'
  }
  else {
    return '$' + paramCount
  }
}

module.exports.fixquery = function (entp, q) {
  var qq = {}

  for (var qp in q) {
    qq[qp] = q[qp]
  }

  if (_.isFunction(qq.id)) {
    delete qq.id
  }

  return qq
}


/**
 * Create a new persistable entity from the entity object. The function adds
 * the value for SENECA_TYPE_COLUMN with hints for type of the serialized objects.
 *
 * @param ent entity
 * @return {Object}
 */
module.exports.makeentp = function (ent, jsonSupport) {
  var entp = {}
  var type = {}
  var fields = ent.fields$()

  fields.forEach(function (field) {
    if (jsonSupport) {
      entp[field] = ent[field]
      return
    }

    if (_.isArray(ent[field])) {
      type[field] = ARRAY_TYPE
    }
    else if (!_.isDate(ent[field]) && _.isObject(ent[field])) {
      type[field] = OBJECT_TYPE
    }

    if (!_.isDate(ent[field]) && _.isObject(ent[field])) {
      entp[field] = JSON.stringify(ent[field])
    }
    else {
      entp[field] = ent[field]
    }
  })

  if (!_.isEmpty(type)) {
    entp[SENECA_TYPE_COLUMN] = JSON.stringify(type)
  }

  return entp
}


/**
 * Create a new entity using a row from database. This function is using type
 * hints from database column SENECA_TYPE_COLUMN to deserialize stored values
 * into proper objects.
 *
 * @param ent entity
 * @param row database row data
 * @return {Entity}
 */
module.exports.makeent = function (ent, row) {
  if (!row) {
    return null
  }

  var entp = {}
  var senecatype = {}
  var fields = _.keys(row)

  if (!_.isUndefined(row[SENECA_TYPE_COLUMN]) && !_.isNull(row[SENECA_TYPE_COLUMN])) {
    senecatype = parseIfJSON(row[SENECA_TYPE_COLUMN])
  }

  if (!_.isUndefined(ent) && !_.isUndefined(row)) {
    fields.forEach(function (field) {
      if (SENECA_TYPE_COLUMN !== field) {
        if (_.isUndefined(senecatype[field])) {
          entp[field] = row[field]
        }
        else if (senecatype[field] === OBJECT_TYPE) {
          entp[field] = parseIfJSON(row[field])
        }
        else if (senecatype[field] === ARRAY_TYPE) {
          entp[field] = parseIfJSON(row[field])
        }
        else if (senecatype[field] === DATE_TYPE) {
          entp[field] = new Date(row[field])
        }
      }
    })
  }

  return ent.make$(entp)
}

module.exports.escapeStr = function (input) {
  if (input instanceof Date) {
    return input
  }
  var str = '' + input
  return str.replace(/[\0\b\t\x08\x09\x1a\n\r"'\\\%]/g, function (char) {
    switch (char) {
      case '\0':
        return '\\0'
      case '\x08':
        return '\\b'
      case '\b':
        return '\\b'
      case '\x09':
        return '\\t'
      case '\t':
        return '\\t'
      case '\x1a':
        return '\\z'
      case '\n':
        return '\\n'
      case '\r':
        return '\\r'
      case '"':
      case '\'':
      case '\\':
      case '%':
        return '\\' + char

    }
  })
}


function parseIfJSON (data) {
  // If the JSON type is used, do not re-parse the already parsed json
  if (_.isString(data)) {
    return JSON.parse(data)
  }
  else if (_.isObject(data)) {
    return data
  }
}


module.exports.tablename = function (entity) {
  var canon = entity.canon$({object: true})

  return (canon.base ? canon.base + '_' : '') + canon.name
}
