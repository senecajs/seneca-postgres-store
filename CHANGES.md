## 2.5.0 2026-10-08

* Seneca 4 prerelease support (4.0.0-rc5 and 4.0.0): the store now uses
  the `entity/init` export of seneca-entity, as Seneca 4 and current
  seneca-entity no longer provide `seneca.store.init`.
* Works with seneca-entity 28: a missing `seneca.entity.state()` means
  no transaction (previously every action threw).
* pg upgraded to ^8.23.1. Tested on Node 24 and 22 against PostgreSQL 18.
* Tests: @hapi/lab 26, @hapi/code 9, seneca-store-test 6. Transaction
  tests run only with seneca-entity 21.x or 22.x.
* Local services through `docker-compose.yml` (`npm run services:up`,
  `npm run services:down`); settings from `SENECA_TEST_PG_*`.
* Documentation reorganised into `docs/` (tutorials, how-to, reference,
  explanation). Removed `.travis.yml` and coveralls.

## 2.3.0 26-08-2016

* Updated dependencies
* Added Seneca 3 and Node 6 support
* Dropped Node 0.10, 0.12, 5 support

## 2.2.1 2016-08-02
* Updated dependencies to be like Seneca ones

## 2.2.0: 2016-07-27
* Updated dependencies

## 2.1.0: 2016-06-08

Updated dependencies
! Updated pg from 4.x.x to 5.x.x

## 2.0.0: 2016-04-05

Removed the internal conversion that the plugin was making from CamelCase column names to Snake case. Backward compatibility is ensured by allowing the user to pass into options two functions named toColumnName() and fromColumnName() that make this conversion. These should implement the CamelCase to Snake case conversion. More details are provided in the [README](https://github.com/senecajs/seneca-postgres-store) in the **Column name transformation, backward compatibility** section.

All query generation code related to basic seneca functionality was moved to [seneca-standard-query](https://github.com/senecajs/seneca-standard-query) and the extended query functionality moved to [seneca-store-query](https://github.com/senecajs/seneca-store-query). This doesn't change functionality but enables functionality reuse into other stores.
