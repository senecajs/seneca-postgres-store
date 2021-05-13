const Seneca = require('seneca')
const Lab = require('@hapi/lab')
const lab = (exports.lab = Lab.script())
const { before, beforeEach, it, describe } = lab
const { expect } = require('code')

const PgStore = require('..')
const DefaultPgConfig = require('./default_config.json')
const Shared = require('seneca-store-test')

describe('tests', () => {
  function makeSenecaForTest() {
    const app = Seneca({
      log: 'test'
    })

    app.use('seneca-entity', { mem_store: false })

    app.use(PgStore, DefaultPgConfig)

    return app
  }

  describe('zzz', () => {
    const app = makeSenecaForTest()

    it('', fin => {
      return app.ready(() => {
        return app.make('foo')
          .data$({ p1: 'vv1', p2: 'vv2' })
          .save$((err, _foo) => {
            if (err) {
              return fin(err)
            }

            return app.make('foo').remove$({ all$: true }, fin)
          })
      })
    })
  })

  describe('yyy', () => {
    const app = makeSenecaForTest()

    before(() => new Promise(fin => app.ready(fin)))

    Shared.yyytest({
      seneca: app,
      script: lab
    })
  })
})

