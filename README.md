![Seneca](http://senecajs.org/files/assets/seneca-logo.png)
> A [Seneca.js](http://senecajs.org) data storage plugin

seneca-postgres-store
=======================

[![npm version][npm-badge]][npm-url]

seneca-postgres-store is a [PostgreSQL][postgresqlorg] database plugin for the [Seneca][seneca] MVP toolkit. The plugin is using the
[node-postgres][nodepg] driver.

Usage:

    var seneca = require('seneca');
    var store = require('seneca-postgres-store');

    var config = {}
    var storeopts = {
      name:'dbname',
      host:'127.0.0.1',
      port:5432,
      username:'user',
      password:'password',
      nolimit: true
    };

    ...

    var si = seneca(config)
    si.use(store, storeopts)
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
The standard Seneca query format is supported:

- `.list$({f1:v1, f2:v2, ...})` implies pseudo-query `f1==v1 AND f2==v2, ...`.

- `.list$({f1:v1, ...}, {sort$:{field1:1}})` means sort by f1, ascending.

- `.list$({f1:v1, ...}, {sort$:{field1:-1}})` means sort by f1, descending.

- `.list$({f1:v1, ...}, {limit$:10})` means only return 10 results.

- `.list$({f1:v1, ...}, {skip$:5})` means skip the first 5.

- `.list$({f1:v1,...}, {fields$:['fd1','f2']})` means only return the listed fields.


## Support for Seneca store V2.0.0

This includes support for more complex queries.


### Comparison query operators

Starting from version version 1.x.x list$ supports also these comparison operators:

- ne$: `.list$({ f1: {ne$: v1} })` for not-equal. 
- eq$: `.list$({ f1: {eq$: v1} })` for equal. 
- lte$: `.list$({ f1: {lte$: 5} })` for less than or equal. 
- lt$: `.list$({ f1: {lt$: 5} })` for less than. 
- gte$: `.list$({ f1: {gte$: 5} })` for greater than or equal. 
- gt$: `.list$({ f1: {gt$: 5} })` for greater than. 
- in$: `.list$({ f1: {in$: [10, 20]} })` for in. in$ operator accepts only values of type array. 
- nin$: `.list$({ f1: {nin$: ['v1', 'v2']} })` for not-in. nin$ operator accepts only values of type array. 


Note: you can use `sort$`, `limit$`, `skip$` and `fields$` together.

Note: you can use any operators described above together.

### Logical query operators

Starting from version version 1.1.x list$ supports also these logical operators:

- or$: `.list$({ or$: [{name: 'something'}, {price: 200}]})`
- and$: `.list$({ and$: [{name: 'something'}, {price: 200}]})`

Note: These logical operators accepts only arrays as values.

Note: These operators can be used together to build more complex queries

Note: These logical operators can be used also with any Comparison query operators described above.

Note: A complex example:

```js
ent.list$( 
  { 
    or$: [
      {name: 'something'}, 
      {
        and$: [
          {price: {gte$: 100}}, 
          {name: 'other'}
        ]
      }, 
      {color: { ne$: 'red' }}
    ], 
    sort$: {name: 1},
    fields$: ['name', 'color']
  }, function(err, list){
    // do something with result...
  } )
```

## Limits

By default queries are limited to 20 values. This can be bypassed by passing the `nolimit` option, which if set to true will not limit any queries.

## Fields

To filter the fields returned from the `list` operation, pass a `fields$` array of column names to return. If no `fields$` are passed, all fields are returned (i.e. `select *` is used). e.g.

    query.fields$ = ['id', 'name']


Note: The implicit id that is generated on save$ has uuid value. To override this you must provide entity.id$ with a desired value.

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


## Contributing
We encourage participation. If you feel you can help in any way, be it with
examples, extra testing, or new features please get in touch.

[npm-badge]: https://badge.fury.io/js/seneca-postgres-store.svg
[npm-url]: https://badge.fury.io/js/seneca-postgres-store
