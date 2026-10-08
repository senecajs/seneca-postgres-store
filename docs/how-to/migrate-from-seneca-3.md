# Migrate from Seneca 3

Goal: run an application that uses this store on Seneca 4.

1. Install Seneca 4 and current seneca-entity:

   ```sh
   npm install seneca@4.0.0-rc5 seneca-entity@28 @seneca/postgres-store
   ```

2. Pass store options through `use()` (or `options.plugin`). Seneca 4
   does not merge top level options named after the plugin.

3. Load `seneca-entity` before the store, with `mem_store: false` if
   you do not want the in-memory store as well.

4. Errors from actions are not wrapped on Seneca 4. Check
   `err.message` for the PostgreSQL message instead of `err.orig`.

5. Transactions (`seneca.entity.begin()`) need seneca-entity 21.x or
   22.x. Later releases removed that API; the store then runs every
   action outside a transaction.

6. Use Node 22 or later.
