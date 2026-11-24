# Migrate from Seneca 3

Goal: run an application that uses this store on Seneca 4.

1. Use Node.js 24 or later.

2. Install Seneca 4, the entity plugin `@seneca/entity` and this store
   under its new name:

   ```sh
   npm uninstall seneca-postgres-store seneca-entity
   npm install seneca@^4 @seneca/entity @seneca/postgres-store
   ```

   `seneca-entity` 28 and earlier do not load on Seneca 4.0.0 (the
   error is `plugin_gubu_shape`). `@seneca/entity` 29 is the entity
   plugin for Seneca 4.

3. Load `@seneca/entity` before the store, by its full name, with
   `mem_store: false` if you do not want the in-memory store as well:

   ```js
   seneca
     .use('@seneca/entity', { mem_store: false })
     .use('@seneca/postgres-store', { name, host, port, username, password })
   ```

4. Pass store options through `use()` (or `options.plugin`). Seneca 4
   does not merge top level options named after the plugin.

5. Errors from actions are not wrapped on Seneca 4. Check
   `err.message` for the PostgreSQL message instead of `err.orig`.

6. `@seneca/entity` has no transaction API, so every action runs
   outside a transaction. The store's transaction support works only
   with `seneca-entity` 21.0.x (`seneca.entity.begin()`), which is not
   a supported setup for this release. See
   [How the store works](../explanation/how-it-works.md#transactions).
