const Seneca = require('seneca')
const Lab = require('@hapi/lab')
const lab = (exports.lab = Lab.script())
const { before, beforeEach, it, describe } = lab
const { expect } = require('code')

const PgStore = require('..')
const DefaultPgConfig = require('./default_config.json')
const Shared = require('seneca-store-test')

const Async = require('async')
const Uuid = require('node-uuid')

const POSTGRES_STORE_NAME = 'postgresql-store'


describe('unbroken', () => { // TODO: Replace the description.
  const si = makeSenecaForTest()

  before(() => {
    return new Promise(done => {
      si.ready(() => {
        si.use(require('seneca-store-query'))
        si.ready(done)
      })
    })
  })

  describe('shared tests', () => { // dbg
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

    it('save with passing an external id', () => new Promise(done => {
      var idPrefix = 'test_'

      si.add({role: 'sql', hook: 'generate_id', target: POSTGRES_STORE_NAME}, function (args, done) {
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
    }))

    it('should support opaque ids (array) and fields$', () => new Promise(done => {
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
    }))
  })

  describe('postgres store API V2.0.0', function () {
    before(() => new Promise(done => {
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
    }))

    it('use not equal ne$', () => new Promise(done => {
      var product = si.make('product')

      product.list$({price: {ne$: 200}, sort$: {price: 1}}, function (err, lst) {
        expect(err).to.not.exist()

        expect(2).to.equal(lst.length)
        expect('apple').to.equal(lst[0].name)
        expect('cherry').to.equal(lst[1].name)
        done()
      })
    }))

    it('use not equal ne$ string', () => new Promise(done => {
      var product = si.make('product')

      product.list$({name: {ne$: 'pear'}, sort$: {price: 1}}, function (err, lst) {
        expect(err).to.not.exist()

        expect(2).to.equal(lst.length)
        expect('apple').to.equal(lst[0].name)
        expect('cherry').to.equal(lst[1].name)
        done()
      })
    }))

    it('use eq$', () => new Promise(done => {
      var product = si.make('product')

      product.list$({price: {eq$: 200}}, function (err, lst) {
        expect(err).to.not.exist()

        expect(1).to.equal(lst.length)
        expect('pear').to.equal(lst[0].name)
        done()
      })
    }))

    it('use eq$ string', () => new Promise(done => {
      var product = si.make('product')

      product.list$({name: {eq$: 'pear'}}, function (err, lst) {
        expect(err).to.not.exist()

        expect(1).to.equal(lst.length)
        expect('pear').to.equal(lst[0].name)
        done()
      })
    }))

    it('use gte$', () => new Promise(done => {
      var product = si.make('product')

      product.list$({price: {gte$: 200}, sort$: {price: 1}}, function (err, lst) {
        expect(err).to.not.exist()

        expect(2).to.equal(lst.length)
        expect('pear').to.equal(lst[0].name)
        expect('cherry').to.equal(lst[1].name)
        done()
      })
    }))

    it('use gt$', () => new Promise(done => {
      var product = si.make('product')

      product.list$({price: {gt$: 200}, sort$: {price: 1}}, function (err, lst) {
        expect(err).to.not.exist()

        expect(1).to.equal(lst.length)
        expect('cherry').to.equal(lst[0].name)
        done()
      })
    }))

    it('use lte$', () => new Promise(done => {
      var product = si.make('product')

      product.list$({price: {lte$: 200}, sort$: {price: 1}}, function (err, lst) {
        expect(err).to.not.exist()

        expect(2).to.equal(lst.length)
        expect('apple').to.equal(lst[0].name)
        expect('pear').to.equal(lst[1].name)
        done()
      })
    }))

    it('use lt$', () => new Promise(done => {
      var product = si.make('product')

      product.list$({price: {lt$: 200}, sort$: {price: 1}}, function (err, lst) {
        expect(err).to.not.exist()

        expect(1).to.equal(lst.length)
        expect('apple').to.equal(lst[0].name)
        done()
      })
    }))

    it('use in$', () => new Promise(done => {
      var product = si.make('product')

      product.list$({price: {in$: [200, 300]}, sort$: {price: 1}}, function (err, lst) {
        expect(err).to.not.exist()

        expect(2).to.equal(lst.length)
        expect('pear').to.equal(lst[0].name)
        expect('cherry').to.equal(lst[1].name)
        done()
      })
    }))

    it('use in$ string', () => new Promise(done => {
      var product = si.make('product')

      product.list$({name: {in$: ['cherry', 'pear']}, sort$: {price: 1}}, function (err, lst) {
        expect(err).to.not.exist()

        expect(2).to.equal(lst.length)
        expect('pear').to.equal(lst[0].name)
        expect('cherry').to.equal(lst[1].name)
        done()
      })
    }))

    it('use in$ one matching', () => new Promise(done => {
      var product = si.make('product')

      product.list$({price: {in$: [200, 500, 700]}, sort$: {price: 1}}, function (err, lst) {
        expect(err).to.not.exist()

        expect(1).to.equal(lst.length)
        expect('pear').to.equal(lst[0].name)
        done()
      })
    }))

    it('use in$ no matching', () => new Promise(done => {
      var product = si.make('product')

      product.list$({price: {in$: [250, 500, 700]}, sort$: {price: 1}}, function (err, lst) {
        expect(err).to.not.exist()

        expect(0).to.equal(lst.length)
        done()
      })
    }))

    it('use nin$ three matching', () => new Promise(done => {
      var product = si.make('product')

      product.list$({price: {nin$: [250, 500, 700]}, sort$: {price: 1}}, function (err, lst) {
        expect(err).to.not.exist()

        expect(3).to.equal(lst.length)
        done()
      })
    }))

    it('use nin$ one matching', () => new Promise(done => {
      var product = si.make('product')

      product.list$({price: {nin$: [200, 500, 300]}, sort$: {price: 1}}, function (err, lst) {
        expect(err).to.not.exist()

        expect(1).to.equal(lst.length)
        expect('apple').to.equal(lst[0].name)
        done()
      })
    }))

    it('use complex in$ and nin$', () => new Promise(done => {
      var product = si.make('product')

      product.list$({price: {nin$: [250, 500, 300], in$: [200, 300]}, sort$: {price: 1}}, function (err, lst) {
        expect(err).to.not.exist()

        expect(1).to.equal(lst.length)
        expect('pear').to.equal(lst[0].name)
        done()
      })
    }))

    it('use nin$ string', () => new Promise(done => {
      var product = si.make('product')

      product.list$({name: {nin$: ['cherry', 'pear']}, sort$: {price: 1}}, function (err, lst) {
        expect(err).to.not.exist()

        expect(1).to.equal(lst.length)
        expect('apple').to.equal(lst[0].name)
        done()
      })
    }))

    it('use or$', () => new Promise(done => {
      var product = si.make('product')

      product.list$({or$: [{name: 'cherry'}, {price: 200}], sort$: {price: 1}}, function (err, lst) {
        expect(err).to.not.exist()

        expect(2).to.equal(lst.length)
        expect('pear').to.equal(lst[0].name)
        expect('cherry').to.equal(lst[1].name)
        done()
      })
    }))

    it('use and$', () => new Promise(done => {
      var product = si.make('product')

      product.list$({and$: [{name: 'cherry'}, {price: 300}], sort$: {price: 1}}, function (err, lst) {
        expect(err).to.not.exist()

        expect(1).to.equal(lst.length)
        expect('cherry').to.equal(lst[0].name)
        done()
      })
    }))

    it('use and$ & or$', () => new Promise(done => {
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
    }))

    it('use and$ & or$ and limit$', () => new Promise(done => {
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
    }))

    it('use and$ & or$ and limit$, fields$ and skip$', () => new Promise(done => {
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
    }))
  })
})

function makeSenecaForTest() {
  const si = Seneca({
    log: 'test'
  })

  si.use('seneca-entity', { mem_store: false })

  si.use(PgStore, DefaultPgConfig)

  return si
}

function clearDb(si) {
  return () => new Promise(done => {
    Async.series([
      function clearFoo(next) {
        si.make('foo').remove$({ all$: true }, next)
      },

      function clearBar(next) {
        si.make('zen', 'moon', 'bar').remove$({ all$: true }, next)
      }
    ], done)
  })
}

function createEntities(si, name, data) {
  return () => {
    return new Promise((done) => {
      Async.each(
        data,
        function (el, next) {
          si.make$(name, el).save$(next)
        },
        done
      )
    })
  }
}

function verify(cb, tests) {
  return (error, out) => {
    if (error) {
      return cb(error)
    }

    try {
      tests(out)
    }
    catch (ex) {
      return cb(ex)
    }

    return cb()
  }
}
