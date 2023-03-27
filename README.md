# Hrana client for TypeScript

This package implements a Hrana client for TypeScript. Hrana is a protocol based on WebSockets that can be used to connect to sqld. It is more efficient than the postgres wire protocol (especially for edge deployments) and it supports interactive stateful SQL connections (called "streams") which are not supported by the HTTP API.

> This package is intended mostly for internal use. Consider using the [`@libsql/client`][libsql-client] package, which will automatically use Hrana if you connect to a `ws://` or `wss://` URL.

[libsql-client]: https://www.npmjs.com/package/@libsql/client

## Usage

```typescript
import * as hrana from "@libsql/hrana-client";

// Open a `hrana.Client`, which works like a connection pool in standard SQL
// databases, but it uses just a single network connection internally
const url = process.env.URL ?? "ws://localhost:8080"; // Address of the sqld server
const jwt = process.env.JWT; // JWT token for authentication
const client = hrana.open(url, jwt);

// Open a `hrana.Stream`, which is an interactive SQL stream. This corresponds
// to a "connection" from other SQL databases
const stream = client.openStream();

// Fetch all rows returned by a SQL statement
const books = await stream.query("SELECT title, year FROM book WHERE author = 'Jane Austen'");
// The rows are returned in an Array...
for (const book of books.rows) {
    // every returned row works as an array (`book[1]`) and as an object (`book.year`)
    console.log(`${book.title} from ${book.year}`);
}

// Fetch a single row
const book = await stream.queryRow("SELECT title, MIN(year) FROM book");
if (book.row !== undefined) {
    console.log(`The oldest book is ${book.row.title} from year ${book.row[1]}`);
}

// Fetch a single value, using a bound parameter
const year = await stream.queryValue(["SELECT MAX(year) FROM book WHERE author = ?", ["Jane Austen"]]);
if (year.value !== undefined) {
    console.log(`Last book from Jane Austen was published in ${year.value}`);
}

// Execute a statement that does not return any rows
const res = await stream.run(["DELETE FROM book WHERE author = ?", ["J. K. Rowling"]])
console.log(`${res.affectedRowCount} books have been cancelled`);

// When you are done, remember to close the client
client.close();
```
