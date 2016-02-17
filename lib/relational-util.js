/* Base class for relational databases */
'use strict'

var _ = require('lodash')
var UpperCaseRegExp = /[A-Z]/g

var SENECA_TYPE_COLUMN = 'seneca'

var OBJECT_TYPE = 'o'
var ARRAY_TYPE = 'a'
var DATE_TYPE = 'd'


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
module.exports.makeentp = function (ent) {
  var entp = {}
  var type = {}
  var fields = ent.fields$()

  fields.forEach(function (field) {
    entp[field] = ent[field]
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
          entp[field] = row[field]
        }
        else if (senecatype[field] === DATE_TYPE) {
          entp[field] = new Date(row[field])
        }
      }
    })
  }

  return ent.make$(entp)
}

module.exports.camelToSnakeCase = function (field) {
  // replace "camelCase" with "camel_case"
  UpperCaseRegExp.lastIndex = 0 // just to be sure. does not seem necessary. String.replace seems to reset the regexp each time.
  return field.replace(UpperCaseRegExp, function (str, offset) {
    return ('_' + str.toLowerCase())
  })
}

module.exports.snakeToCamelCase = function (column) {
  // replace "snake_case" with "snakeCase"
  var arr = column.split('_')
  var field = arr[0]
  for (var i = 1; i < arr.length; i++) {
    field += arr[i][0].toUpperCase() + arr[i].slice(1, arr[i].length)
  }

  return field
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
