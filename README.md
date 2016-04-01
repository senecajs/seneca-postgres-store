![Seneca](http://senecajs.org/files/assets/seneca-logo.png)
> A [Seneca.js](http://senecajs.org) data storage plugin

seneca-postgres-store
=======================

[![npm version][npm-badge]][npm-url]
[![Build Status][travis-badge]][travis-url]
[![Code Climate][codeclimate-badge]][codeclimate-url]
[![Dependency Status][david-badge]][david-url]
[![Gitter][gitter-badge]][gitter-url]

seneca-postgres-store is a [PostgreSQL][postgresqlorg] database plugin for the [Seneca][seneca] MVP toolkit. The plugin is using the [node-postgres][nodepg] driver.
For query generation it uses internally the [seneca-standard-store][standard-store] plugin and the standard functionality can be extended by using the [seneca-store-query][store-query] plugin.

Usage:

    var Seneca = require('seneca');
    var store = require('seneca-postgres-store');

    var DBConfig = {
      name: 'senecatest',
      host: 'localhost',
      username: 'senecatest',
      password: 'senecatest',
      port: 5432
    }
    ...

    var si = Seneca(DBConfig)
    si.use(require('seneca-postgres-store'), DBConfig)
    si.ready(function() {
      var product = si.make('product')
      ...
    })
    ...

[postgresqlorg]: http://www.postgresql.org/
[seneca]: http://senecajs.org/
[nodepg]: https://github.com/brianc/node-postgres

## Usage
You don't use this module directly. It provides an underlying data storage engine for the Seneca entity API:

```js
var entity = seneca.make$('typename')
entity.someproperty = "something"
entity.anotherproperty = 100

entity.save$(function (err, entity) { ... })
entity.load$({id: ...}, function (err, entity) { ... })
entity.list$({property: ...}, function (err, entity) { ... })
entity.remove$({id: ...}, function (err, entity) { ... })
```

### Query Support

The standard Seneca query format is supported. See the [seneca-standard-store][standard-store] plugin for more details.

## Extend Query Support

By using the [seneca-store-query][store-query] plugin its query capabilities can be extended. See the plugin page for more details.

## Limits

By default queries are limited to 20 values. This can be bypassed by passing the `nolimit` option, which if set to true will not limit any queries.

## Fields

To filter the fields returned from the `list` operation, pass a `fields$` array of column names to return. If no `fields$` are passed, all fields are returned (i.e. `select *` is used). e.g.

    query.fields$ = ['id', 'name']


Note: The implicit id that is generated on save$ has uuid value. To override this you must provide entity.id$ with a desired value.

### Custom ID generator

To generate custom IDs it is exposed a seneca action pattern hook that can be overwritten:


```js
seneca.add({role: 'sql', hook: 'generate_id', target: <store name>}, function (args, done) {
  return done(null, {id: idPrefix + Uuid()})
})

```

### Native Driver
As with all seneca stores, you can access the native driver, in this case, the `pg`
`connection` object using `entity.native$(function (err, connectionPool, release) {...})`.
Please make sure that you release the connection after using it.

```
entity.native$( function (err, client, releaseConnection){
  // ... you can use client
  // ... then release connection
  releaseConnection()
} )
```

## Running tests

To run the tests you need to have the docker image built and running, that is made executing `npm run build` then `npm run start`
In another console execute `npm test`

## Contributing
We encourage participation. If you feel you can help in any way, be it with
examples, extra testing, or new features please get in touch.


[npm-badge]: https://img.shields.io/npm/v/seneca-postgres-store.svg
[npm-url]: https://npmjs.com/package/seneca-postgres-store
[travis-badge]: https://api.travis-ci.org/senecajs/seneca-postgres-store.svg
[travis-url]: https://travis-ci.org/senecajs/seneca-postgres-store
[david-badge]: https://david-dm.org/senecajs/seneca-postgres-store.svg
[david-url]: https://david-dm.org/senecajs/seneca-postgres-store
[codeclimate-badge]: https://codeclimate.com/github/senecajs/seneca-postgres-store/badges/gpa.svg
[codeclimate-url]: https://codeclimate.com/github/senecajs/seneca-postgres-store
[gitter-badge]: https://badges.gitter.im/Join%20Chat.svg
[gitter-url]: https://gitter.im/senecajs/seneca
[standard-store]: https://github.com/senecajs/seneca-standard-store
[store-query]: https://github.com/senecajs/seneca-store-query
