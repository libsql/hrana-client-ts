import * as hrana from "@libsql/hrana-client";

// Open a `hrana.Client`, which works like a connection pool in standard SQL
// databases, but it uses just a single network connection internally
const url = process.env.URL ?? "ws://localhost:2023"; // Address of the sqld server
const jwt = process.env.JWT; // JWT token for authentication
const client = hrana.open(url, jwt);

// Open a `hrana.Stream`, which is an interactive SQL stream. This corresponds
// to a "connection" from other SQL databases
const stream = client.openStream();

await stream.execute(`CREATE TABLE book (
    id INTEGER PRIMARY KEY NOT NULL,
    author TEXT NOT NULL,
    title TEXT NOT NULL,
    year INTEGER NOT NULL
)`);
await stream.execute(`INSERT INTO book (author, title, year) VALUES
    ('Jane Austen', 'Sense and Sensibility', 1811),
    ('Jane Austen', 'Pride and Prejudice', 1813),
    ('Jane Austen', 'Mansfield Park', 1814),
    ('Jane Austen', 'Emma', 1815),
    ('Jane Austen', 'Persuasion', 1818),
    ('Jane Austen', 'Lady Susan', 1871),
    ('Daniel Defoe', 'Robinson Crusoe', 1719),
    ('Daniel Defoe', 'A Journal of the Plague Year', 1722),
    ('J. K. Rowling', 'Harry Potter and the Philosopher''s Stone', 1997),
    ('J. K. Rowling', 'The Casual Vacancy', 2012),
    ('J. K. Rowling', 'The Ickabog', 2020)
`);

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
const res = await stream.execute(["DELETE FROM book WHERE author = ?", ["J. K. Rowling"]])
console.log(`${res.rowsAffected} books have been cancelled`);

// When you are done, remember to close the client
client.close();
