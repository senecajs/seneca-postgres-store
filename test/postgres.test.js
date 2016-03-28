'use strict'

var Seneca = require('seneca')

var Lab = require('lab')
var lab = exports.lab = Lab.script()
var Code = require('code')
var expect = Code.expect

var Async = require('async')
var Uuid = require('node-uuid')

var describe = lab.describe
var before = lab.before
var it = lab.it
var beforeEach = lab.beforeEach

var Shared = require('seneca-store-test')
var DefaultConfig = require('./default_config.json')

var si = Seneca()

var storeName = 'postgresql-store'
var actionRole = 'sql'

function clearDb (si) {
  return function clear (done) {
    Async.series([
      function clearFoo (next) {
        si.make('foo').remove$({ all$: true }, next)
      },
      function clearBar (next) {
        si.make('zen', 'moon', 'bar').remove$({ all$: true }, next)
      }
    ], done)
  }
}

function createEntities (si, name, data) {
  return function create (done) {
    Async.each(data, function (el, next) {
      si.make$(name, el).save$(next)
    }, done)
  }
}

function verify (cb, tests) {
  return function (error, out) {
    if (error) {
      return cb(error)
    }

    try {
      tests(out)
    }
    catch (ex) {
      return cb(ex)
    }

    cb()
  }
}

describe('Basic Test', function () {
  before({}, function (done) {
    si.use(require('..'), DefaultConfig)
    si.ready(function () {
      si.use(require('seneca-store-query'))
      si.ready(done)
    })
  })

  Shared.basictest({
    seneca: si,
    script: lab
  })

  Shared.sorttest({
    seneca: si,
    script: lab
  })

  Shared.limitstest({
    seneca: si,
    script: lab
  })

  Shared.sqltest({
    seneca: si,
    script: lab
  })
})

describe('postgres', function () {
  beforeEach(clearDb(si))
  beforeEach(createEntities(si, 'foo', [{
    id$: 'foo1',
    p1: 'v1'
  }, {
    id$: 'foo2',
    p1: 'v2',
    p2: 'z2'
  }]))

  it('save with passing an external id', function (done) {
    var idPrefix = 'test_'
    si.add({role: actionRole, hook: 'generate_id', target: storeName}, function (args, done) {
      return done(null, {id: idPrefix + Uuid()})
    })

    var foo = si.make('foo')
    foo.p1 = 'v1'
    foo.p2 = 'v2'

    foo.save$(function (err, foo1) {
      expect(err).to.not.exist()
      expect(foo1.id).to.exist()
      expect(foo1.id).to.startWith(idPrefix)

      foo1.load$(foo1.id, function (err, foo2) {
        expect(err).to.not.exist()
        expect(foo2).to.exist()
        expect(foo2.id).to.equal(foo1.id)
        expect(foo2.p1).to.equal('v1')
        expect(foo2.p2).to.equal('v2')

        done()
      })
    })
  })

  it('should support opaque ids (array) and fields$', function (done) {
    var foo = si.make('foo')
    foo.list$({ids: ['foo1', 'foo2'], fields$: ['p1']}, verify(done, function (res) {
      expect(2).to.equal(res.length)
      expect(res[0].p1).to.equal('v1')
      expect(res[0].p2).to.not.exist()
      expect(res[0].p3).to.not.exist()
      expect(res[1].p1).to.equal('v2')
      expect(res[1].p2).to.not.exist()
      expect(res[1].p3).to.not.exist()
    }))
  })
})

describe('postgres store API V2.0.0', function () {
  before(function (done) {
    var Product = si.make('product')

    Async.series([
      function clear (next) {
        Product.remove$({all$: true}, next)
      },
      function create (next) {
        var products = [
          Product.make$({name: 'apple', price: 100}),
          Product.make$({name: 'pear', price: 200}),
          Product.make$({name: 'cherry', price: 300})
        ]

        function saveproduct (product, saved) {
          product.save$(saved)
        }

        Async.forEach(products, saveproduct, next)
      }
    ], function (err) {
      expect(err).to.not.exist()
      done()
    })
  })

  it('use not equal ne$', function (done) {
    var product = si.make('product')

    product.list$({price: {ne$: 200}, sort$: {price: 1}}, function (err, lst) {
      expect(err).to.not.exist()

      expect(2).to.equal(lst.length)
      expect('apple').to.equal(lst[0].name)
      expect('cherry').to.equal(lst[1].name)
      done()
    })
  })

  it('use not equal ne$ string', function (done) {
    var product = si.make('product')

    product.list$({name: {ne$: 'pear'}, sort$: {price: 1}}, function (err, lst) {
      expect(err).to.not.exist()

      expect(2).to.equal(lst.length)
      expect('apple').to.equal(lst[0].name)
      expect('cherry').to.equal(lst[1].name)
      done()
    })
  })

  it('use eq$', function (done) {
    var product = si.make('product')

    product.list$({price: {eq$: 200}}, function (err, lst) {
      expect(err).to.not.exist()

      expect(1).to.equal(lst.length)
      expect('pear').to.equal(lst[0].name)
      done()
    })
  })

  it('use eq$ string', function (done) {
    var product = si.make('product')

    product.list$({name: {eq$: 'pear'}}, function (err, lst) {
      expect(err).to.not.exist()

      expect(1).to.equal(lst.length)
      expect('pear').to.equal(lst[0].name)
      done()
    })
  })

  it('use gte$', function (done) {
    var product = si.make('product')

    product.list$({price: {gte$: 200}, sort$: {price: 1}}, function (err, lst) {
      expect(err).to.not.exist()

      expect(2).to.equal(lst.length)
      expect('pear').to.equal(lst[0].name)
      expect('cherry').to.equal(lst[1].name)
      done()
    })
  })

  it('use gt$', function (done) {
    var product = si.make('product')

    product.list$({price: {gt$: 200}, sort$: {price: 1}}, function (err, lst) {
      expect(err).to.not.exist()

      expect(1).to.equal(lst.length)
      expect('cherry').to.equal(lst[0].name)
      done()
    })
  })

  it('use lte$', function (done) {
    var product = si.make('product')

    product.list$({price: {lte$: 200}, sort$: {price: 1}}, function (err, lst) {
      expect(err).to.not.exist()

      expect(2).to.equal(lst.length)
      expect('apple').to.equal(lst[0].name)
      expect('pear').to.equal(lst[1].name)
      done()
    })
  })

  it('use lt$', function (done) {
    var product = si.make('product')

    product.list$({price: {lt$: 200}, sort$: {price: 1}}, function (err, lst) {
      expect(err).to.not.exist()

      expect(1).to.equal(lst.length)
      expect('apple').to.equal(lst[0].name)
      done()
    })
  })

  it('use in$', function (done) {
    var product = si.make('product')

    product.list$({price: {in$: [200, 300]}, sort$: {price: 1}}, function (err, lst) {
      expect(err).to.not.exist()

      expect(2).to.equal(lst.length)
      expect('pear').to.equal(lst[0].name)
      expect('cherry').to.equal(lst[1].name)
      done()
    })
  })

  it('use in$ string', function (done) {
    var product = si.make('product')

    product.list$({name: {in$: ['cherry', 'pear']}, sort$: {price: 1}}, function (err, lst) {
      expect(err).to.not.exist()

      expect(2).to.equal(lst.length)
      expect('pear').to.equal(lst[0].name)
      expect('cherry').to.equal(lst[1].name)
      done()
    })
  })

  it('use in$ one matching', function (done) {
    var product = si.make('product')

    product.list$({price: {in$: [200, 500, 700]}, sort$: {price: 1}}, function (err, lst) {
      expect(err).to.not.exist()

      expect(1).to.equal(lst.length)
      expect('pear').to.equal(lst[0].name)
      done()
    })
  })

  it('use in$ no matching', function (done) {
    var product = si.make('product')

    product.list$({price: {in$: [250, 500, 700]}, sort$: {price: 1}}, function (err, lst) {
      expect(err).to.not.exist()

      expect(0).to.equal(lst.length)
      done()
    })
  })

  it('use nin$ three matching', function (done) {
    var product = si.make('product')

    product.list$({price: {nin$: [250, 500, 700]}, sort$: {price: 1}}, function (err, lst) {
      expect(err).to.not.exist()

      expect(3).to.equal(lst.length)
      done()
    })
  })

  it('use nin$ one matching', function (done) {
    var product = si.make('product')

    product.list$({price: {nin$: [200, 500, 300]}, sort$: {price: 1}}, function (err, lst) {
      expect(err).to.not.exist()

      expect(1).to.equal(lst.length)
      expect('apple').to.equal(lst[0].name)
      done()
    })
  })

  it('use complex in$ and nin$', function (done) {
    var product = si.make('product')

    product.list$({price: {nin$: [250, 500, 300], in$: [200, 300]}, sort$: {price: 1}}, function (err, lst) {
      expect(err).to.not.exist()

      expect(1).to.equal(lst.length)
      expect('pear').to.equal(lst[0].name)
      done()
    })
  })

  it('use nin$ string', function (done) {
    var product = si.make('product')

    product.list$({name: {nin$: ['cherry', 'pear']}, sort$: {price: 1}}, function (err, lst) {
      expect(err).to.not.exist()

      expect(1).to.equal(lst.length)
      expect('apple').to.equal(lst[0].name)
      done()
    })
  })

  it('use or$', function (done) {
    var product = si.make('product')

    product.list$({or$: [{name: 'cherry'}, {price: 200}], sort$: {price: 1}}, function (err, lst) {
      expect(err).to.not.exist()

      expect(2).to.equal(lst.length)
      expect('pear').to.equal(lst[0].name)
      expect('cherry').to.equal(lst[1].name)
      done()
    })
  })

  it('use and$', function (done) {
    var product = si.make('product')

    product.list$({and$: [{name: 'cherry'}, {price: 300}], sort$: {price: 1}}, function (err, lst) {
      expect(err).to.not.exist()

      expect(1).to.equal(lst.length)
      expect('cherry').to.equal(lst[0].name)
      done()
    })
  })

  it('use and$ & or$', function (done) {
    var product = si.make('product')

    product.list$({
      or$: [{price: {gte$: 200}}, {and$: [{name: 'cherry'}, {price: 300}]}],
      sort$: {price: 1}
    }, function (err, lst) {
      expect(err).to.not.exist()

      expect(2).to.equal(lst.length)
      expect('pear').to.equal(lst[0].name)
      expect('cherry').to.equal(lst[1].name)
      done()
    })
  })

  it('use and$ & or$ and limit$', function (done) {
    var product = si.make('product')

    product.list$({
      or$: [{price: {gte$: 200}}, {and$: [{name: 'cherry'}, {price: 300}]}],
      sort$: {price: 1},
      limit$: 1,
      fields$: ['name']
    }, function (err, lst) {
      expect(err).to.not.exist()

      expect(1).to.equal(lst.length)
      expect('pear').to.equal(lst[0].name)
      expect(lst[0].price).to.not.exist()
      done()
    })
  })

  it('use and$ & or$ and limit$, fields$ and skip$', function (done) {
    var product = si.make('product')

    product.list$({
      price: {gte$: 200},
      sort$: {price: 1},
      limit$: 1,
      fields$: ['name'],
      skip$: 1
    }, function (err, lst) {
      expect(err).to.not.exist()

      expect(1).to.equal(lst.length)
      expect('cherry').to.equal(lst[0].name)
      expect(lst[0].price).to.not.exist()
      done()
    })
  })
})
