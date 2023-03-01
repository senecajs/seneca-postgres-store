const Seneca = require('seneca')
const Lab = require('@hapi/lab')
const lab = (exports.lab = Lab.script())
const { before, beforeEach, afterEach, describe, it } = lab
const { expect } = require('code')

const PgStore = require('..')
const DbConfig = require('./support/db/config')
const Shared = require('seneca-store-test')

const Async = require('async')
const Uuid = require('uuid').v4
const Util = require('util')

const POSTGRES_STORE_NAME = 'postgresql-store'


describe('seneca postgres plugin', () => {
  describe('shared tests', () => {
    const si = makeSenecaForTest()

    before(() => {
      return new Promise(done => {
        si.ready(done)
      })
    })

    describe('basic tests', () => {
      Shared.basictest({
        seneca: si,
        senecaMerge: makeSenecaForTest({ postgres_opts: { merge: false } }),
        script: lab
      })
    })

    describe('sort tests', () => {
      Shared.sorttest({
        seneca: si,
        script: lab
      })
    })

    describe('limit tests', () => {
      Shared.limitstest({
        seneca: si,
        script: lab
      })
    })

    describe('sql tests', () => {
      Shared.sqltest({
        seneca: si,
        script: lab
      })
    })

    describe('upsert tests', () => {
      Shared.upserttest({
        seneca: si,
        script: lab
      })
    })
  })

  describe('postgres', function () {
    const si = makeSenecaForTest()

    before(() => {
      return new Promise(done => {
        si.ready(done)
      })
    })

    beforeEach(clearDb(si))

    beforeEach(createEntities(si, 'foo', [{
      id$: 'foo1',
      p1: 'v1'
    }, {
      id$: 'foo2',
      p1: 'v2',
      p2: 'z2'
    }]))

    it('save with passing an external id', () => new Promise((resolve, reject) => {
      const idPrefix = 'test_' + Uuid()

      si.add({role: 'sql', hook: 'generate_id', target: POSTGRES_STORE_NAME}, function (args, done) {
        return done(null, {id: idPrefix + Uuid()})
      })

      const foo = si.make('foo')
      foo.p1 = 'v1'
      foo.p2 = 'v2'

      return foo.save$(function (err, foo1) {
        if (err) {
          return reject(err)
        }

        try {
          expect(foo1.id).to.exist()
          expect(foo1.id).to.startWith(idPrefix)
        } catch (err) {
          return reject(err)
        }

        return foo1.load$(foo1.id, function (err, foo2) {
          if (err) {
            return reject(err)
          }

          try {
            expect(foo2).to.exist()
            expect(foo2.id).to.equal(foo1.id)
            expect(foo2.p1).to.equal('v1')
            expect(foo2.p2).to.equal('v2')
          } catch (err) {
            return reject(err)
          }

          return resolve()
        })
      })
    }))

    it('should support opaque ids (array) and fields$', () => new Promise((resolve, reject) => {
      const foo = si.make('foo')

      return foo.list$({id: ['foo1', 'foo2'], fields$: ['p1']}, function (err, out) {
        if (err) {
          return reject(err)
        }

        const res = sortBy(out, x => x.p1)

        try {
          expect(2).to.equal(res.length)
          expect(res[0].p1).to.equal('v1')
          expect(res[0].p2).to.not.exist()
          expect(res[0].p3).to.not.exist()
          expect(res[1].p1).to.equal('v2')
          expect(res[1].p2).to.not.exist()
          expect(res[1].p3).to.not.exist()
        } catch (err) {
          return reject(err)
        }

        return resolve()
      })
    }))
  })

  describe('transaction', function () {
    const si = makeSenecaForTest({entity_opts: { transaction: {active:true} }})
    si.use('promisify')
    
    before(() => {
      return new Promise(done => {
        si.ready(done)
      })
    })

    beforeEach(clearDb(si))

    afterEach(clearDb(si))

    it('happy', async () => {
      let s0 = await si.entity.begin()
      await s0.entity('foo').data$({p1:'t1'}).save$()
      let tx0 = await s0.entity.end()

      expect(tx0).include({
        handle: { name: 'postgres' },
        result: { done: true },
      })
      // console.log(tx0)
      // console.dir(tx0.trace)

      let foos = await si.entity('foo').list$()
      expect(foos.length).equal(1)
      expect(foos[0].p1).equal('t1')
    })

    
    it('rollback', async () => {
      let s0 = await si.entity.begin()

      await s0.entity('foo').data$({p1:'t1'}).save$()

      let tx0 = await s0.entity.rollback()

      expect(tx0).include({
        handle: { name: 'postgres' },
        result: { done: false },
      })
      // console.log(tx0)
      // console.dir(tx0.trace)

      let foos = await si.entity('foo').list$()
      expect(foos.length).equal(0)
    })


    it('rollback-on-error', async () => {
      si.message('foo:red', async function foo_red(msg) {
        await this.entity('foo').data$({p1:'t1'}).save$()
        throw new Error('BAD')
        await this.entity('foo').data$({p1:'t2'}).save$()
        return {ok:true}
      })

      let s0 = await si.entity.begin()

      // console.log(s0.entity)
      
      try {
        await s0.post('foo:red')
        expect(false).toEqual(true)
      }
      catch(err) {
        // console.log(err.message)

        // Data NOT saved as rolled back
        let foos = await s0.entity('foo').list$()
        // console.log('FOOS', foos)
        expect(foos.length).equal(0)
      
        let t0 = s0.entity.state()
        // console.log(t0.transaction.trace)
        expect(t0.transaction.trace.length).equal(1)
      }
    })

    

    // TODO: preserved in children
    
  })

  
  describe('postgres store API V2.0.0', function () {
    const si = makeSenecaForTest()

    before(() => {
      return new Promise(done => {
        si.ready(done)
      })
    })

    beforeEach(clearDb(si))

    afterEach(clearDb(si))

    beforeEach(() => new Promise((resolve, reject) => {
      const Product = si.make('products')

      return Async.series(
        [
          function clear(next) {
            Product.remove$({all$: true}, next)
          },

          function create(next) {
            const products = [
              Product.make$({label: 'apple', price: '100'}),
              Product.make$({label: 'pear', price: '200'}),
              Product.make$({label: 'cherry', price: '300'})
            ]

            function saveproduct(product, next) {
              product.save$(next)
            }

            Async.forEach(products, saveproduct, next)
          }
        ],

        function (err) {
          if (err) {
            return reject(err)
          }

          return resolve()
        }
      )
    }))

    it('use not equal ne$', () => new Promise((resolve, reject) => {
      const product = si.make('products')

      return product.list$({price: {ne$: '200'}, sort$: {price: 1}}, function (err, lst) {
        if (err) {
          return reject(err)
        }

        try {
          expect(2).to.equal(lst.length)
          expect('apple').to.equal(lst[0].label)
          expect('cherry').to.equal(lst[1].label)
        } catch (err) {
          return reject(err)
        }

        return resolve()
      })
    }))

    it('use not equal ne$ string', () => new Promise((resolve, reject) => {
      const product = si.make('products')

      return product.list$({label: {ne$: 'pear'}, sort$: {price: 1}}, function (err, lst) {
        if (err) {
          return reject(err)
        }

        try {
          expect(2).to.equal(lst.length)
          expect('apple').to.equal(lst[0].label)
          expect('cherry').to.equal(lst[1].label)
        } catch (err) {
          return reject(err)
        }

        return resolve()
      })
    }))

    it('use eq$', () => new Promise((resolve, reject) => {
      const product = si.make('products')

      return product.list$({price: {eq$: '200'}}, function (err, lst) {
        if (err) {
          return reject(err)
        }

        try {
          expect(1).to.equal(lst.length)
          expect('pear').to.equal(lst[0].label)
        } catch (err) {
          return reject(err)
        }

        return resolve()
      })
    }))

    it('use eq$ string', () => new Promise((resolve, reject) => {
      const product = si.make('products')

      return product.list$({label: {eq$: 'pear'}}, function (err, lst) {
        if (err) {
          return reject(err)
        }

        try {
          expect(1).to.equal(lst.length)
          expect('pear').to.equal(lst[0].label)
        } catch (err) {
          return reject(err)
        }

        return resolve()
      })
    }))

    it('use gte$', () => new Promise((resolve, reject) => {
      const product = si.make('products')

      return product.list$({price: {gte$: '200'}, sort$: {price: 1}}, function (err, lst) {
        if (err) {
          return reject(err)
        }

        try {
          expect(2).to.equal(lst.length)
          expect('pear').to.equal(lst[0].label)
          expect('cherry').to.equal(lst[1].label)
        } catch (err) {
          return reject(err)
        }

        return resolve()
      })
    }))

    it('use gt$', () => new Promise((resolve, reject) => {
      const product = si.make('products')

      return product.list$({price: {gt$: '200'}, sort$: {price: 1}}, function (err, lst) {
        if (err) {
          return reject(err)
        }

        try {
          expect(1).to.equal(lst.length)
          expect('cherry').to.equal(lst[0].label)
        } catch (err) {
          return reject(err)
        }

        return resolve()
      })
    }))

    it('use lte$', () => new Promise((resolve, reject) => {
      const product = si.make('products')

      return product.list$({price: {lte$: '200'}, sort$: {price: 1}}, function (err, lst) {
        if (err) {
          return reject(err)
        }

        try {
          expect(2).to.equal(lst.length)
          expect('apple').to.equal(lst[0].label)
          expect('pear').to.equal(lst[1].label)
        } catch (err) {
          return reject(err)
        }

        return resolve()
      })
    }))

    it('use lt$', () => new Promise((resolve, reject) => {
      const product = si.make('products')

      return product.list$({price: {lt$: '200'}, sort$: {price: 1}}, function (err, lst) {
        if (err) {
          return reject(err)
        }

        try {
          expect(1).to.equal(lst.length)
          expect('apple').to.equal(lst[0].label)
        } catch (err) {
          return reject(err)
        }

        return resolve()
      })
    }))

    it('use in$', () => new Promise((resolve, reject) => {
      const product = si.make('products')

      return product.list$({price: {in$: ['200', '300']}, sort$: {price: 1}}, function (err, lst) {
        if (err) {
          return reject(err)
        }

        try {
          expect(2).to.equal(lst.length)
          expect('pear').to.equal(lst[0].label)
          expect('cherry').to.equal(lst[1].label)
        } catch (err) {
          return reject(err)
        }

        return resolve()
      })
    }))

    it('use in$ string', () => new Promise((resolve, reject) => {
      const product = si.make('products')

      return product.list$({label: {in$: ['cherry', 'pear']}, sort$: {price: 1}}, function (err, lst) {
        if (err) {
          return reject(err)
        }

        try {
          expect(2).to.equal(lst.length)
          expect('pear').to.equal(lst[0].label)
          expect('cherry').to.equal(lst[1].label)
        } catch (err) {
          return reject(err)
        }

        return resolve()
      })
    }))

    it('use in$ one matching', () => new Promise((resolve, reject) => {
      const product = si.make('products')

      return product.list$({price: {in$: ['200', '500', '700']}, sort$: {price: 1}}, function (err, lst) {
        if (err) {
          return reject(err)
        }

        try {
          expect(1).to.equal(lst.length)
          expect('pear').to.equal(lst[0].label)
        } catch (err) {
          return reject(err)
        }

        return resolve()
      })
    }))

    it('use in$ no matching', () => new Promise((resolve, reject) => {
      const product = si.make('products')

      return product.list$({price: {in$: ['250', '500', '700']}, sort$: {price: 1}}, function (err, lst) {
        if (err) {
          return reject(err)
        }

        try {
          expect(err).to.not.exist()
          expect(0).to.equal(lst.length)
        } catch (err) {
          return reject(err)
        }

        return resolve()
      })
    }))

    it('use nin$ three matching', () => new Promise((resolve, reject) => {
      const product = si.make('products')

      return product.list$({price: {nin$: ['250', '500', '700']}, sort$: {price: 1}}, function (err, lst) {
        if (err) {
          return(err)
        }

        try {
          expect(3).to.equal(lst.length)
        } catch (err) {
          return reject(err)
        }

        return resolve()
      })
    }))

    it('use nin$ one matching', () => new Promise((resolve, reject) => {
      const product = si.make('products')

      return product.list$({price: {nin$: ['200', '500', '300']}, sort$: {price: 1}}, function (err, lst) {
        if (err) {
          return(err)
        }

        try {
          expect(1).to.equal(lst.length)
          expect('apple').to.equal(lst[0].label)
        } catch (err) {
          return reject(err)
        }

        return resolve()
      })
    }))

    it('use complex in$ and nin$', () => new Promise((resolve, reject) => {
      const product = si.make('products')

      return product.list$({
        price: {nin$: ['250', '500', '300'],
          in$: ['200', '300']
        },
        sort$: {price: 1}
      }, function (err, lst) {
        if (err) {
          return(err)
        }

        try {
          expect(1).to.equal(lst.length)
          expect('pear').to.equal(lst[0].label)
        } catch (err) {
          return reject(err)
        }

        return resolve()
      })
    }))

    it('use nin$ string', () => new Promise((resolve, reject) => {
      const product = si.make('products')

      return product.list$({label: {nin$: ['cherry', 'pear']}, sort$: {price: 1}}, function (err, lst) {
        if (err) {
          return(err)
        }

        try {
          expect(1).to.equal(lst.length)
          expect('apple').to.equal(lst[0].label)
        } catch (err) {
          return reject(err)
        }

        return resolve()
      })
    }))

    it('use or$', () => new Promise((resolve, reject) => {
      const product = si.make('products')

      return product.list$({or$: [{label: 'cherry'}, {price: '200'}], sort$: {price: 1}}, function (err, lst) {
        if (err) {
          return(err)
        }

        try {
          expect(2).to.equal(lst.length)
          expect('pear').to.equal(lst[0].label)
          expect('cherry').to.equal(lst[1].label)
        } catch (err) {
          return reject(err)
        }

        return resolve()
      })
    }))

    it('use and$', () => new Promise((resolve, reject) => {
      const product = si.make('products')

      return product.list$({and$: [{label: 'cherry'}, {price: '300'}], sort$: {price: 1}}, function (err, lst) {
        if (err) {
          return reject(err)
        }

        try {
          expect(1).to.equal(lst.length)
          expect('cherry').to.equal(lst[0].label)
        } catch (err) {
          return reject(err)
        }

        return resolve()
      })
    }))

    it('use and$ & or$', () => new Promise((resolve, reject) => {
      const product = si.make('products')

      return product.list$({
        or$: [{price: {gte$: '200'}}, {and$: [{label: 'cherry'}, {price: '300'}]}],
        sort$: {price: 1}
      }, function (err, lst) {
        if (err) {
          return reject(err)
        }

        try {
          expect(2).to.equal(lst.length)
          expect('pear').to.equal(lst[0].label)
          expect('cherry').to.equal(lst[1].label)
        } catch (err) {
          return reject(err)
        }

        return resolve()
      })
    }))

    it('use and$ & or$ and limit$', () => new Promise((resolve, reject) => {
      const product = si.make('products')

      return product.list$({
        or$: [{price: {gte$: '200'}}, {and$: [{label: 'cherry'}, {price: '300'}]}],
        sort$: {price: 1},
        limit$: 1,
        fields$: ['label']
      }, function (err, lst) {
        if (err) {
          return reject(err)
        }

        try {
          expect(1).to.equal(lst.length)
          expect('pear').to.equal(lst[0].label)
          expect(lst[0].price).to.not.exist()
        } catch (err) {
          return reject(err)
        }

        return resolve()
      })
    }))

    it('use and$ & or$ and limit$, fields$ and skip$', () => new Promise((resolve, reject) => {
      const product = si.make('products')

      return product.list$({
        price: {gte$: '200'},
        sort$: {price: 1},
        limit$: 1,
        fields$: ['label'],
        skip$: 1
      }, function (err, lst) {
        if (err) {
          return reject(err)
        }

        try {
          expect(1).to.equal(lst.length)
          expect('cherry').to.equal(lst[0].label)
          expect(lst[0].price).to.not.exist()
        } catch (err) {
          return reject(err)
        }

        return resolve()
      })
    }))

    describe('#save$', () => {
      describe('auto_increment$:true', () => {
        describe('normally', () => {
          it('relies on the database to generate the id', () => new Promise((resolve, reject) => {
            si.make('auto_incrementors')
              .data$({ value: 37 })
              .save$({ auto_increment$: true }, function (err, ent) {
                if (err) {
                  return reject(err)
                }

                try {
                  expect(ent).to.exist()
                  expect(typeof ent.id).to.equal('number')
                  expect(ent.value).to.equal(37)
                } catch (err) {
                  return reject(err)
                }

                return si.make('auto_incrementors').load$(ent.id, function (err, ent) {
                  if (err) {
                    return reject(err)
                  }

                  try {
                    expect(ent).to.exist()
                    expect(typeof ent.id).to.equal('number')
                    expect(ent.value).to.equal(37)
                  } catch (err) {
                    return reject(err)
                  }

                  return resolve()
                })
              })
          }))
        })

        describe('when upserting', () => {
          describe('no match exists', () => {
            it('relies on the database to generate the id', () => new Promise((resolve, reject) => {
              si.make('auto_incrementors')
                .data$({ value: 37 })
                .save$({ auto_increment$: true, upsert$: ['value'] }, function (err, ent) {
                  if (err) {
                    return reject(err)
                  }

                  try {
                    expect(ent).to.exist()
                    expect(typeof ent.id).to.equal('number')
                    expect(ent.value).to.equal(37)
                  } catch (err) {
                    return reject(err)
                  }

                  return si.make('auto_incrementors').load$(ent.id, function (err, ent) {
                    if (err) {
                      return reject(err)
                    }

                    try {
                      expect(ent).to.exist()
                      expect(typeof ent.id).to.equal('number')
                      expect(ent.value).to.equal(37)
                    } catch (err) {
                      return reject(err)
                    }

                    return resolve()
                  })
                })
            }))
          })
        })
      })
    })
  })

  describe('Column Names conversions', function () {
    describe('Default CamelCase to snake_case conversion', function () {
      const si = makeSenecaForTest()

      before(() => {
        return new Promise(done => {
          si.ready(done)
        })
      })

      beforeEach(clearDb(si))

      beforeEach(createEntities(si, 'foo', [{
        fooBar: 'fooBar',
        bar_foo: 'bar_foo'
      }]))

      it('should not alter CamelCase column names, in list$', () => new Promise((resolve, reject) => {
        const foo = si.make('foo')

        return foo.list$({native$: 'SELECT * FROM foo WHERE "fooBar" = \'fooBar\''}, function (err, res) {
          if (err) {
            return reject(err)
          }

          try {
            expect(res.length).to.equal(1)
            expect(res[0].fooBar).to.equal('fooBar')
          } catch (err) {
            return reject(err)
          }

          return resolve()
        })
      }))

      it('should not alter snake_case column names, in list$', () => new Promise((resolve, reject) => {
        const foo = si.make('foo')

        return foo.list$({native$: 'SELECT * FROM foo WHERE bar_foo = \'bar_foo\''}, function (err, res) {
          if (err) {
            return reject(err)
          }

          try {
            expect(res.length).to.equal(1)
            expect(res[0].bar_foo).to.equal('bar_foo')

            return resolve()
          } catch (err) {
            return reject(err)
          }
        })
      }))

      it('should not alter snake_case column names, in list$', () => new Promise((resolve, reject) => {
        const foo = si.make('foo')

        return foo.list$({ bar_foo: 'bar_foo' }, function (err, res) {
          if (err) {
            return reject(err)
          }

          try {
            expect(res.length).to.equal(1)
            expect(res[0].bar_foo).to.equal('bar_foo')
          } catch (err) {
            return reject(err)
          }

          return resolve()
        })
      }))
    })

    describe('Custom CamelCase to snake_case conversion', function () {
      const si = makeSenecaForTest({
        postgres_opts: {
          toColumnName: camelToSnakeCase,
          fromColumnName: snakeToCamelCase
        }
      })

      before(() => {
        return new Promise(done => {
          si.ready(done)
        })
      })

      beforeEach(clearDb(si))

      beforeEach(createEntities(si, 'foo', [{
        barFoo: 'barFoo'
      }]))

      it('should convert the CamelCase column name to snake case, in list$', () => new Promise((resolve, reject) => {
        const foo = si.make('foo')

        return foo.list$({native$: 'SELECT * FROM foo WHERE "bar_foo" = \'barFoo\''}, function (err, res) {
          if (err) {
            return reject(err)
          }

          try {
            expect(res.length).to.equal(1)
            expect(res[0].barFoo).to.equal('barFoo')
          } catch (err) {
            return reject(err)
          }

          return resolve()
        })
      }))

      it('should convert the CamelCase column name to snake case, in list$', () => new Promise((resolve, reject) => {
        const foo = si.make('foo')

        return foo.list$({ barFoo: 'barFoo' }, function (err, res) {
          if (err) {
            return reject(err)
          }

          try {
            expect(res.length).to.equal(1)
            expect(res[0].barFoo).to.equal('barFoo')
          } catch (err) {
            return reject(err)
          }

          return resolve()
        })
      }))


      const UpperCaseRegExp = /[A-Z]/g

      // Replace "camelCase" with "camel_case"
      function camelToSnakeCase (field) {
        UpperCaseRegExp.lastIndex = 0
        return field.replace(UpperCaseRegExp, function (str, offset) {
          return ('_' + str.toLowerCase())
        })
      }

      // Replace "snake_case" with "snakeCase"
      function snakeToCamelCase (column) {
        const arr = column.split('_')
        let field = arr[0]

        for (let i = 1; i < arr.length; i++) {
          field += arr[i][0].toUpperCase() + arr[i].slice(1, arr[i].length)
        }

        return field
      }
    })
  })
})

function makeSenecaForTest(opts = {}) {
  const si = Seneca().test()

  const { entity_opts = {}, postgres_opts = {} } = opts
  
  si.use('seneca-entity', {
    mem_store: false,
    ...entity_opts
  })

  si.use(PgStore, { ...DbConfig, ...postgres_opts })

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
      },

      function clearProduct(next) {
        si.make('products').remove$({ all$: true }, next)
      },

      function clearAutoIncrementors(next) {
        si.make('auto_incrementors').remove$({ all$: true }, next)
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

function sortBy(ary, f) {
  return [...ary].sort((a, b) => {
    const x = f(a)
    const y = f(b)

    if (x < y) {
      return -1
    }

    if (x > y) {
      return 1
    }

    return 0
  })
}

