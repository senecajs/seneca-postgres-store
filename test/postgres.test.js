'use strict'

var assert = require('assert')
var seneca = require('seneca')
var Lab = require('lab')
var lab = exports.lab = Lab.script()
var Async = require('async')

var describe = lab.describe
var before = lab.before
var it = lab.it

var shared = require('seneca-store-test')
var default_config = require('./default_config.json')

var si = seneca()
si.use(require('..'), default_config)

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

  shared.sqltest({
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
          assert(!err)
          assert.equal(product.id, '12345')
          done()
        })
  })
})

describe('postgres store API V2.0.0', function () {
  before(function (done) {
    var Product = si.make('product')

    Async.series([
      function clear (next) {
        Product.remove$({ all$: true }, next)
      },
      function create (next) {
        var products = [
          Product.make$({ name: 'apple', price: 100 }),
          Product.make$({ name: 'pear', price: 200 }),
          Product.make$({ name: 'cherry', price: 300 })
        ]

        function saveproduct (product, saved) {
          product.save$(saved)
        }

        Async.forEach(products, saveproduct, next)
      }
    ], function (err) {
      assert(!err)
      done()
    })
  })

  it('use not equal', function (done) {
    var product = si.make('product')

    product.list$({ price: {ne$: 200}, sort$: {price: 1} }, function (err, lst) {
      assert(!err)

      assert.equal(2, lst.length)
      assert.equal('apple', lst[0].name)
      assert.equal('cherry', lst[1].name)
      done()
    })
  })
})

