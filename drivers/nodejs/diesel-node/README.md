# DieselDB Node.js Driver (`diesel-node`)

Pure-TypeScript, **zero-runtime-dependency** driver for DieselDB exposing a
**`pg`-package-compatible** API over the libpq wire protocol (the subset
spoken by `diesel.LibpqServer`).

## Installation

```bash
npm install diesel-node
```

## Usage

```js
const { Client } = require('diesel-node');

const client = new Client({
  host: 'localhost',
  port: 5432,
  database: 'testdb',
  user: 'alice',
  password: 'secret',
});
// or: new Client('postgres://alice:secret@localhost:5432/testdb')

await client.connect();
await client.query('CREATE TABLE demo (ID INTEGER, NAME STRING)');
await client.query('INSERT INTO demo (ID, NAME) VALUES ($1, $2)',
                   [1, 'Alice']);
const res = await client.query('SELECT ID, NAME FROM demo WHERE ID = $1', [1]);
console.log(res.rows);          // [{ ID: 1, NAME: 'Alice' }]
console.log(res.fields);        // [{ name: 'ID', dataTypeID: 23, ... }, ...]
await client.end();
```

`query()` returns a Promise (the `pg` Result shape: `command`, `rowCount`,
`rows` as objects keyed by column name, `fields`). Parameter binding uses
`$1..$n` placeholders and is performed **client-side** (the server's extended
protocol Parse/Bind/Execute is a stub).

Error handling: protocol/database errors throw `DbError` (with `code` set to
the server's SQLSTATE - currently always `00000`), a subclass of `Error`.

## Compatibility notes / limitations

* Only trust and cleartext-password authentication are supported (no SCRAM).
* Empty `SELECT` results send no `RowDescription`, so `fields` is empty and
  `rows` is `[]` for zero-row results.
* `rowCount` for DML reflects the server's (currently hardcoded) counts.
* The server executes each simple query independently - there is no
  multi-statement transaction state.

## Development

```bash
npm install        # installs the TypeScript devDependency
npm run build      # tsc -> dist/
npm test           # build + node --test test/
```

## Publishing (maintainers)

```bash
npm run build
npm publish --access public
```

CI: tag `driver-node-v*` triggers `.github/workflows/publish-drivers.yml`
(requires the `NPM_TOKEN` repository secret).
