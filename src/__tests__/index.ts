import * as hrana from "..";

function withClient(f: (c: hrana.Client) => Promise<void>): () => Promise<void> {
    return async () => {
        const c = hrana.open(process.env.URL ?? "ws://localhost:2023", process.env.JWT);
        try {
            await f(c);
        } finally {
            c.close();
        }
    };
}

test("Stream.queryValue() with value", withClient(async (c) => {
    const s = c.openStream();
    const res = await s.queryValue("SELECT 1 AS one");
    expect(res.value).toStrictEqual(1);
    expect(res.columnNames).toStrictEqual(["one"]);
    expect(res.affectedRowCount).toStrictEqual(0);
}));

test("Stream.queryValue() without value", withClient(async (c) => {
    const s = c.openStream();
    const res = await s.queryValue("SELECT 1 AS one WHERE 0 = 1");
    expect(res.value).toStrictEqual(undefined);
    expect(res.columnNames).toStrictEqual(["one"]);
    expect(res.affectedRowCount).toStrictEqual(0);
}));

test("Stream.queryRow() with row", withClient(async (c) => {
    const s = c.openStream();
    
    const res = await s.queryRow(
        "SELECT 1 AS one, 'elephant' AS two, 42.5 AS three, NULL as four");
    expect(res.columnNames).toStrictEqual(["one", "two", "three", "four"]);
    expect(res.affectedRowCount).toStrictEqual(0);

    const row = res.row as hrana.Row;
    expect(row[0]).toStrictEqual(1);
    expect(row[1]).toStrictEqual("elephant");
    expect(row[2]).toStrictEqual(42.5);
    expect(row[3]).toStrictEqual(null);

    expect(row[0]).toStrictEqual(row.one);
    expect(row[1]).toStrictEqual(row.two);
    expect(row[2]).toStrictEqual(row.three);
    expect(row[3]).toStrictEqual(row.four);
}));

test("Stream.queryRow() without row", withClient(async (c) => {
    const s = c.openStream();
    
    const res = await s.queryValue("SELECT 1 AS one WHERE 0 = 1");
    expect(res.value).toStrictEqual(undefined);
    expect(res.columnNames).toStrictEqual(["one"]);
    expect(res.affectedRowCount).toStrictEqual(0);
}));

test("Stream.query()", withClient(async (c) => {
    const s = c.openStream();

    await s.execute("BEGIN");
    await s.execute("DROP TABLE IF EXISTS t");
    await s.execute("CREATE TABLE t (one, two, three, four)");
    await s.execute(
        `INSERT INTO t VALUES
            (1, 'elephant', 42.5, NULL),
            (2, 'hippopotamus', '123', 0.0)`
    );

    const res = await s.query("SELECT * FROM t ORDER BY one");
    expect(res.affectedRowCount).toStrictEqual(0);

    expect(res.rows.length).toStrictEqual(2);

    const row0 = res.rows[0];
    expect(row0[0]).toStrictEqual(1);
    expect(row0[1]).toStrictEqual("elephant");
    expect(row0["three"]).toStrictEqual(42.5);
    expect(row0["four"]).toStrictEqual(null);

    const row1 = res.rows[1];
    expect(row1["one"]).toStrictEqual(2);
    expect(row1["two"]).toStrictEqual("hippopotamus");
    expect(row1[2]).toStrictEqual("123");
    expect(row1[3]).toStrictEqual(0.0);
}));

test("Stream.execute()", withClient(async (c) => {
    const s = c.openStream();

    let res = await s.execute("BEGIN");
    expect(res.affectedRowCount).toStrictEqual(0);

    res = await s.execute("DROP TABLE IF EXISTS t");
    expect(res.affectedRowCount).toStrictEqual(0);

    res = await s.execute("CREATE TABLE t (num, word)");
    expect(res.affectedRowCount).toStrictEqual(0);

    res = await s.execute("INSERT INTO t VALUES (1, 'one'), (2, 'two'), (3, 'three')");
    expect(res.affectedRowCount).toStrictEqual(3);
    expect(res.lastInsertRowid).toBeDefined();
    expect(res.lastInsertRowid).not.toStrictEqual("0");

    const rowsRes = await s.query("SELECT * FROM t ORDER BY num");
    expect(rowsRes.rows.length).toStrictEqual(3);
    expect(rowsRes.affectedRowCount).toStrictEqual(0);
    expect(rowsRes.columnNames).toStrictEqual(["num", "word"]);

    res = await s.execute("DELETE FROM t WHERE num >= 2");
    expect(res.affectedRowCount).toStrictEqual(2);

    res = await s.execute("UPDATE t SET num = 4, word = 'four'");
    expect(res.affectedRowCount).toStrictEqual(1);

    res = await s.execute("DROP TABLE t");
    expect(res.affectedRowCount).toStrictEqual(0);

    await s.execute("COMMIT");
}));

test("Stream.executeRaw()", withClient(async (c) => {
    const s = c.openStream();

    let res = await s.executeRaw({
        "sql": "SELECT 1 as one, ? as two, NULL as three",
        "args": [{"type": "text", "value": "1+1"}],
        "want_rows": true,
    });

    expect(res.cols).toStrictEqual([
        {"name": "one"},
        {"name": "two"},
        {"name": "three"},
    ]);
    expect(res.rows).toStrictEqual([
        [
            {"type": "integer", "value": "1"},
            {"type": "text", "value": "1+1"},
            {"type": "null"},
        ],
    ]);
}));

test("positional args", withClient(async (c) => {
    const s = c.openStream();
    const res = await s.queryRow(["SELECT ?, ?3, ?2", ['one', null, 3]]);
    const row = res.row!;
    expect(row[0]).toStrictEqual('one');
    expect(row[1]).toStrictEqual(3);
    expect(row[2]).toStrictEqual(null);
}));

test("named args", withClient(async (c) => {
    const s = c.openStream();
    const res = await s.queryRow(["SELECT :one, @two, $three", {":one": 10, "two": 20, "$three": 30}]);
    const row = res.row!;
    expect(row[0]).toStrictEqual(10);
    expect(row[1]).toStrictEqual(20);
    expect(row[2]).toStrictEqual(30);
}));

test("Stmt without arguments", withClient(async (c) => {
    const s = c.openStream();
    const res = await s.queryValue(new hrana.Stmt("SELECT 1"));
    expect(res.value).toStrictEqual(1);
}));

test("Stmt.bindIndexes()", withClient(async (c) => {
    const s = c.openStream();
    const res = await s.queryValue(new hrana.Stmt("SELECT ? || ?").bindIndexes(["a", "b"]));
    expect(res.value).toStrictEqual("ab");
}));

test("Stmt.bindIndex()", withClient(async (c) => {
    const s = c.openStream();
    const res = await s.queryRow(new hrana.Stmt("SELECT ?, ?").bindIndex(2, "b"));
    const row = res.row!;
    expect(row[0]).toStrictEqual(null);
    expect(row[1]).toStrictEqual("b");
}));

test("Stmt.bindName()", withClient(async (c) => {
    const s = c.openStream();
    const res = await s.queryValue(new hrana.Stmt("SELECT $x").bindName("x", 10));
    expect(res.value).toStrictEqual(10);
}));

test("ArrayBuffer as argument", withClient(async (c) => {
    const s = c.openStream();
    const res = await s.queryValue(["SELECT length(?)", [new ArrayBuffer(42)]]);
    expect(res.value).toStrictEqual(42);
}));

test("Uint8Array as argument", withClient(async (c) => {
    const s = c.openStream();
    const res = await s.queryValue(["SELECT length(?)", [new Uint8Array(42)]]);
    expect(res.value).toStrictEqual(42);
}));

test("ArrayBuffer as result", withClient(async (c) => {
    const s = c.openStream();
    const res = await s.queryValue("SELECT randomblob(38)");
    expect(res.value).toBeInstanceOf(ArrayBuffer);
    expect((res.value as ArrayBuffer).byteLength).toStrictEqual(38);
}));

test("ArrayBuffer roundtrip", withClient(async (c) => {
    const buf = new ArrayBuffer(256);
    const array = new Uint8Array(buf);
    for (let i = 0; i < 256; ++i) {
        array[i] = i;
    }

    const s = c.openStream();
    const res = await s.queryValue(["SELECT ?", [buf]]);
    expect(res.value).toStrictEqual(buf);
}));

test("bigint as argument", withClient(async (c) => {
    const s = c.openStream();
    const res = await s.queryValue(["SELECT ?", [-123n]]);
    expect(res.value).toStrictEqual("-123");
}));

test("Date as argument", withClient(async (c) => {
    const s = c.openStream();
    const res = await s.queryValue(["SELECT ?", [new Date(2023, 0)]]);
    expect(res.value).toStrictEqual(1672527600000);
}));

test("RegExp as argument", withClient(async (c) => {
    const s = c.openStream();
    const res = await s.queryValue(["SELECT ?", [/.*/]]);
    expect(res.value).toStrictEqual("/.*/");
}));

test("unsafe integer", withClient(async (c) => {
    const s = c.openStream();
    await expect(s.queryValue("SELECT 9007199254740992")).rejects.toBeInstanceOf(RangeError);
}));

test("response error", withClient(async (c) => {
    const s = c.openStream();
    await expect(s.queryValue("SELECT")).rejects.toBeInstanceOf(hrana.ResponseError);
}));

test("last insert rowid", withClient(async (c) => {
    const s = c.openStream();

    await s.execute("BEGIN");
    await s.execute("DROP TABLE IF EXISTS t");
    await s.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");

    let res = await s.execute("INSERT INTO t VALUES (123)");
    expect(res.lastInsertRowid).toStrictEqual("123");

    res = await s.execute("INSERT INTO t VALUES (9223372036854775807)");
    expect(res.lastInsertRowid).toStrictEqual("9223372036854775807");

    res = await s.execute("INSERT INTO t VALUES (-9223372036854775808)");
    expect(res.lastInsertRowid).toStrictEqual("-9223372036854775808");
}));

test("column names", withClient(async (c) => {
    const s = c.openStream();

    const rows = await s.query("SELECT 1 AS one, 2 AS two");
    expect(rows.columnNames).toStrictEqual(["one", "two"]);

    const res = await s.execute("SELECT 1 AS one, 2 AS two");
    expect(res.columnNames).toStrictEqual(["one", "two"]);
}));

test("concurrent streams are separate", withClient(async (c) => {
    const s1 = c.openStream();
    await s1.execute("DROP TABLE IF EXISTS t");
    await s1.execute("CREATE TABLE t (number)");
    await s1.execute("INSERT INTO t VALUES (1)");

    const s2 = c.openStream();

    await s1.execute("BEGIN");

    await s2.execute("BEGIN");
    await s2.execute("INSERT INTO t VALUES (10)");

    expect((await s1.queryValue("SELECT SUM(number) FROM t")).value).toStrictEqual(1);
    expect((await s2.queryValue("SELECT SUM(number) FROM t")).value).toStrictEqual(11);
}));

test("concurrent operations are correctly ordered", withClient(async (c) => {
    const s = c.openStream();
    await s.execute("DROP TABLE IF EXISTS t");
    await s.execute("CREATE TABLE t (stream, value)");

    async function stream(streamId: number): Promise<void> {
        const s = c.openStream();

        let value = "s" + streamId;
        await s.execute(["INSERT INTO t VALUES (?, ?)", [streamId, value]]);

        const promises: Array<Promise<hrana.ValueResult>> = [];
        const expectedValues = [];
        for (let i = 0; i < 10; ++i) {
            const promise = s.queryValue([
                "UPDATE t SET value = value || ? WHERE stream = ? RETURNING value",
                ["_" + i, streamId],
            ]);
            value = value + "_" + i;
            promises.push(promise);
            expectedValues.push(value);
        }

        for (let i = 0; i < promises.length; ++i) {
            expect((await promises[i]).value).toStrictEqual(expectedValues[i]);
        }

        s.close();
    }

    const promises = [];
    for (let i = 0; i < 10; ++i) {
        promises.push(stream(i));
    }
    await Promise.all(promises);
}));
