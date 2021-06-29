const Assert = require('assert')

const Q = {
  /*
  insertstm(args) {
    const { into, values, escapeIdentifier } = args

    const col_names = Object.keys(values)
    const col_vals = Object.values(values)


    let bindings = []
    let sql = ''


    sql += 'insert into ' + escapeIdentifier(into) + ' '

    const safe_col_names = col_names.map(col_name => escapeIdentifier(col_name))
    sql += '(' + safe_col_names.join(', ') + ') '

    const val_placeholders = col_vals.map(_ => '?')
    sql += 'values (' + val_placeholders.join(', ') + ') '
    bindings = bindings.concat(col_vals)

    sql += 'returning *'


    return { sql, bindings }
  },
  */

  selectstm(args) {
    const {
      escapeIdentifier,
      from,
      columns = '*',
      where = null,
      offset = null,
      limit = null,
      order_by = null
    } = args


    let bindings = []
    let sql = ''
    let param_no = 1


    sql += 'select '


    if ('*' === columns) {
      sql += '*'
    } else {
      const safe_columns = columns.map(col_name => escapeIdentifier(col_name))
      sql += safe_columns.join(', ')
    }


    sql += ' from ' + escapeIdentifier(from)


    if (null != where) {
      const where_q = wherestm({
        where,
        escapeIdentifier,
        first_param_no: param_no
      })

      sql += ' where ' + where_q.text
      bindings = bindings.concat(where_q.values)
      param_no = where_q.next_param_no
    }


    if (null != order_by) {
      const order_q = orderbystm({
        order_by,
        escapeIdentifier,
        first_param_no: param_no
      })

      sql += ' order by ' + order_q.text
      bindings = bindings.concat(order_q.values)
      param_no = order_q.next_param_no
    }


    if (null != limit) {
      sql += ' limit ' + valuePlaceholder(param_no++)
      bindings.push(limit)
    }


    if (null != offset) {
      sql += ' offset ' + valuePlaceholder(param_no++)
      bindings.push(offset)
    }


    return { text: sql, values: bindings }
  }
}


function valuePlaceholder(param_no) {
  Assert.strictEqual(typeof param_no, 'number', 'param_no')
  return '$' + param_no
}


function wherestm(args) {
  const {
    where,
    escapeIdentifier,
    first_param_no = 1
  } = args

  const update_all = 0 === Object.keys(where).length


  let sql = ''
  let bindings = []
  let param_no = first_param_no


  if (update_all) {
    sql += 'true'
  } else {
    let first_where = true

    for (const where_col in where) {
      const where_val = where[where_col]

      if (!first_where) {
        sql += ' and '
      }

      if (Array.isArray(where_val)) {
        const val_placeholders = where_val.map(_ => valuePlaceholder(param_no++)).join(', ')

        if (0 === val_placeholders.length) {
          sql += 'false'
        } else {
          sql += escapeIdentifier(where_col) + ' in (' + val_placeholders + ')'
          bindings = bindings.concat(where_val)
        }
      } else if (null == where_val) {
          sql += escapeIdentifier(where_col) + ' is null'
      } else {
        sql += escapeIdentifier(where_col) + ' = ' + valuePlaceholder(param_no++)
        bindings.push(where_val)
      }

      first_where = false
    }
  }

  return { text: sql, values: bindings, next_param_no: param_no }
}


function orderbystm(args) {
  const {
    order_by,
    escapeIdentifier,
    first_param_no = 1
  } = args


  let sql = ''
  let bindings = []
  let param_no = first_param_no


  let first_pair = true

  for (const order_col in order_by) {
    if (!first_pair) {
      sql += ', '
    }

    first_pair = false


    const order_val = order_by[order_col]
    const order = 0 <= order_val ? 'asc' : 'desc'

    sql += escapeIdentifier(order_col) + ' ' + order
  }


  return { text: sql, values: bindings, next_param_no: param_no }
}


module.exports = Q
