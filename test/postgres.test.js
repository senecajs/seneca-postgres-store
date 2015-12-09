'use strict'

var assert = require('assert')
var seneca = require('seneca')
var Lab = require('lab')
var lab = exports.lab = Lab.script()

var describe = lab.describe
var it = lab.it

var shared = require('seneca-store-test')

var si = seneca()
si.use(require('..'), {
  name: 'senecatest',
  host: '127.0.0.1',
  port: 5432,
  username: 'senecatest',
  password: 'senecatest',
  options: { }
})

describe('Basic Test', function () {
  shared.basictest({
    seneca: si,
    script: lab
  })

  shared.sorttest({
    seneca: si,
    script: lab
  })

  shared.limitstest({
    seneca: si,
    script: lab
  })
})

describe('postgres', function () {
  it('save with passing an id$', function (done) {
    var product = si.make('foo')

    product.id$ = '12345'
    product.p1 = 'pear'

    si.act({role: 'entity', cmd: 'save', ent: product},
      function (err, product) {
        console.log(arguments)
        assert(!err)
        assert.equal(product.id, '12345')
        done()
      })
  })
})
