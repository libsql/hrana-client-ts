import type { Response } from "@libsql/isomorphic-fetch";
import { fetch, Request } from "@libsql/isomorphic-fetch";

import * as hrana from "..";

const url = process.env.URL ?? "ws://localhost:8080";
const jwt = process.env.JWT;

const isWs = url.startsWith("ws:") || url.startsWith("wss:");
const isHttp = url.startsWith("http:") || url.startsWith("https:");

// HACK: patch the client to try Hrana 3 over HTTP with JSON, too (by default, only Protobuf is used)
import { checkEndpoints } from "../http/client.js";
checkEndpoints.push({
    versionPath: "v3",
    pipelinePath: "v3/pipeline",
    cursorPath: "v3/cursor",
    version: 3,
    encoding: "json",
});

function withClient(f: (c: hrana.Client) => Promise<void>, config?: hrana.ClientConfig): () => Promise<void> {
    return async () => {
        let client: hrana.Client;
        if (isWs) {
            client = hrana.openWs(url, jwt, 3, config);
        } else if (isHttp) {
            client = hrana.openHttp(url, jwt, undefined, 3, config);
        } else {
            throw new Error("expected either ws or http URL");
        }
        try {
            await f(client);
        } finally {
            client.close();
        }
    };
}

function withWsClient(f: (c: hrana.WsClient) => Promise<void>): () => Promise<void> {
    return async () => {
        const client = hrana.openWs(url, jwt, 3);
        try {
            await f(client);
        } finally {
            client.close();
        }
    };
}

function withHttpClient(f: (c: hrana.HttpClient) => Promise<void>): () => Promise<void> {
    return async () => {
        const client = hrana.openHttp(url, jwt, undefined, 3);
        try {
            await f(client);
        } finally {
            client.close();
        }
    };
}

let version = 2;
if (process.env.VERSION) {
    version = parseInt(process.env.VERSION, 10);
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

    await s.run("BEGIN");
    await s.run("DROP TABLE IF EXISTS t");
    await s.run("CREATE TABLE t (one, two, three, four)");
    await s.run(
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

test("Stream.run()", withClient(async (c) => {
    const s = c.openStream();

    let res = await s.run("BEGIN");
    expect(res.affectedRowCount).toStrictEqual(0);

    res = await s.run("DROP TABLE IF EXISTS t");
    expect(res.affectedRowCount).toStrictEqual(0);

    res = await s.run("CREATE TABLE t (num, word)");
    expect(res.affectedRowCount).toStrictEqual(0);

    res = await s.run("INSERT INTO t VALUES (1, 'one'), (2, 'two'), (3, 'three')");
    expect(res.affectedRowCount).toStrictEqual(3);
    expect(res.lastInsertRowid).toBeDefined();
    expect(res.lastInsertRowid).not.toStrictEqual(0n);

    const rowsRes = await s.query("SELECT * FROM t ORDER BY num");
    expect(rowsRes.rows.length).toStrictEqual(3);
    expect(rowsRes.affectedRowCount).toStrictEqual(0);
    expect(rowsRes.columnNames).toStrictEqual(["num", "word"]);

    res = await s.run("DELETE FROM t WHERE num >= 2");
    expect(res.affectedRowCount).toStrictEqual(2);

    res = await s.run("UPDATE t SET num = 4, word = 'four'");
    expect(res.affectedRowCount).toStrictEqual(1);

    res = await s.run("DROP TABLE t");
    expect(res.affectedRowCount).toStrictEqual(0);

    await s.run("COMMIT");
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
    expect(res.value).toStrictEqual(-123);
}));

test("Date as argument", withClient(async (c) => {
    const s = c.openStream();
    const res = await s.queryValue(["SELECT ?", [new Date("2023-01-01Z")]]);
    expect(res.value).toStrictEqual(1672531200000);
}));

test("RegExp as argument", withClient(async (c) => {
    const s = c.openStream();
    const res = await s.queryValue(["SELECT ?", [/.*/]]);
    expect(res.value).toStrictEqual("/.*/");
}));

describe("returned integers", () => {
    describe("'number' int mode", () => {
        test("integer returned as number", withClient(async (c) => {
            c.intMode = "number";
            const s = c.openStream();
            const res = await s.queryValue("SELECT 42");
            expect(typeof res.value).toStrictEqual("number");
            expect(res.value).toStrictEqual(42);
        }));

        test("unsafe integer", withClient(async (c) => {
            const s = c.openStream();
            s.intMode = "number";
            await expect(s.queryValue("SELECT 9007199254740992")).rejects.toBeInstanceOf(RangeError);
        }));
    });

    describe("'bigint' int mode", () => {
        test("integer returned as bigint", withClient(async (c) => {
            c.intMode = "bigint";
            const s = c.openStream();
            const res = await s.queryValue("SELECT 42");
            expect(typeof res.value).toStrictEqual("bigint");
            expect(res.value).toStrictEqual(42n);
        }));

        test("large integer", withClient(async (c) => {
            const s = c.openStream();
            s.intMode = "bigint";
            const res = await s.queryValue("SELECT 9007199254740992");
            await expect(res.value).toStrictEqual(9007199254740992n);
        }));
    });

    describe("'string' int mode", () => {
        test("integer returned as string", withClient(async (c) => {
            c.intMode = "string";
            const s = c.openStream();
            const res = await s.queryValue("SELECT 42");
            expect(typeof res.value).toStrictEqual("string");
            expect(res.value).toStrictEqual("42");
        }));

        test("large integer", withClient(async (c) => {
            const s = c.openStream();
            s.intMode = "string";
            const res = await s.queryValue("SELECT 9007199254740992");
            await expect(res.value).toStrictEqual("9007199254740992");
        }));
    });
});

describe("returned booleans", () => {
    const columnName = 'isActive';
    describe("booleans are JS integers", () => {
        test('without config', withClient(async (c) => {
            const s = c.openStream();
            await s.run("BEGIN");
            await s.run("DROP TABLE IF EXISTS t");
            await s.run(`CREATE TABLE t (id INTEGER PRIMARY KEY, ${columnName} BOOLEAN)`);
            await s.run("INSERT INTO t VALUES (1, true)");
            await s.run("INSERT INTO t VALUES (2, false)");
            await s.run("COMMIT");

            const resTrue = await s.queryRow(`SELECT ${columnName} FROM t WHERE id = 1`);
            const valTrue = resTrue.row?.[columnName];
            expect(typeof valTrue).toStrictEqual("number");
            expect(valTrue).toStrictEqual(1);

            const resFalse = await s.queryRow(`SELECT ${columnName} FROM t WHERE id = 2`);
            const valFalse = resFalse.row?.[columnName];
            expect(typeof valFalse).toStrictEqual("number");
            expect(valFalse).toStrictEqual(0);
        }));

        test('with config', withClient(async (c) => {
            const s = c.openStream();
            const resTrue = await s.queryRow(`SELECT ${columnName} FROM t WHERE id = 1`);
            const valTrue = resTrue.row?.[columnName];
            expect(typeof valTrue).toStrictEqual("number");
            expect(valTrue).toStrictEqual(1);

            const resFalse = await s.queryRow(`SELECT ${columnName} FROM t WHERE id = 2`);
            const valFalse = resFalse.row?.[columnName];
            expect(typeof valFalse).toStrictEqual("number");
            expect(valFalse).toStrictEqual(0);
        }, { castBooleans: false }));
    });

    describe("booleans are JS booleans", () => {
        test('with config', withClient(async (c) => {
            const s = c.openStream();
            const resTrue = await s.queryRow(`SELECT ${columnName} FROM t WHERE id = 1`);
            const valTrue = resTrue.row?.[columnName];
            expect(typeof valTrue).toStrictEqual("boolean");
            expect(valTrue).toStrictEqual(true);

            const resFalse = await s.queryRow(`SELECT ${columnName} FROM t WHERE id = 2`);
            const valFalse = resFalse.row?.[columnName];
            expect(typeof valFalse).toStrictEqual("boolean");
            expect(valFalse).toStrictEqual(false);

            await s.run("BEGIN");
            await s.run("DROP TABLE t");
            await s.run("COMMIT");
        }, { castBooleans: true }));
    });
});

test("response error", withClient(async (c) => {
    const s = c.openStream();
    await expect(s.queryValue("SELECT")).rejects.toBeInstanceOf(hrana.ResponseError);
}));

test("last insert rowid", withClient(async (c) => {
    const s = c.openStream();

    await s.run("BEGIN");
    await s.run("DROP TABLE IF EXISTS t");
    await s.run("CREATE TABLE t (id INTEGER PRIMARY KEY)");

    let res = await s.run("INSERT INTO t VALUES (123)");
    expect(res.lastInsertRowid).toStrictEqual(123n);

    res = await s.run("INSERT INTO t VALUES (9223372036854775807)");
    expect(res.lastInsertRowid).toStrictEqual(9223372036854775807n);

    res = await s.run("INSERT INTO t VALUES (-9223372036854775808)");
    expect(res.lastInsertRowid).toStrictEqual(-9223372036854775808n);
}));

test("column names", withClient(async (c) => {
    const s = c.openStream();

    const rows = await s.query("SELECT 1 AS one, 2 AS two");
    expect(rows.columnNames).toStrictEqual(["one", "two"]);

    const res = await s.run("SELECT 1 AS one, 2 AS two");
    expect(res.columnNames).toStrictEqual(["one", "two"]);
}));

(version >= 2 ? test : test.skip)("column decltypes", withClient(async (c) => {
    await c.getVersion();
    const s = c.openStream();
    await s.run("BEGIN");
    await s.run("DROP TABLE IF EXISTS t");
    await s.run("CREATE TABLE t (a TEXT, b int NOT NULL, c FURRY BUNNY)");

    const res = await s.query("SELECT a, b, c, a + b AS sum FROM t");
    expect(res.columnDecltypes).toStrictEqual(["TEXT", "INT", "FURRY BUNNY", undefined]);
}));

test("concurrent streams are separate", withClient(async (c) => {
    const s1 = c.openStream();
    await s1.run("DROP TABLE IF EXISTS t");
    await s1.run("CREATE TABLE t (number)");
    await s1.run("INSERT INTO t VALUES (1)");

    const s2 = c.openStream();

    await s1.run("BEGIN");

    await s2.run("BEGIN");
    await s2.run("INSERT INTO t VALUES (10)");

    expect((await s1.queryValue("SELECT SUM(number) FROM t")).value).toStrictEqual(1);
    expect((await s2.queryValue("SELECT SUM(number) FROM t")).value).toStrictEqual(11);
}));

test("concurrent operations are correctly ordered", withClient(async (c) => {
    const s = c.openStream();
    await s.run("DROP TABLE IF EXISTS t");
    await s.run("CREATE TABLE t (stream, value)");

    async function stream(streamId: number): Promise<void> {
        const s = c.openStream();

        let value = "s" + streamId;
        await s.run(["INSERT INTO t VALUES (?, ?)", [streamId, value]]);

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

describe("many stream operations", () => {
    test("immediately after each other", withClient(async (c) => {
        const s = c.openStream();
        const promises: Array<Promise<hrana.ValueResult>> = [];
        for (let i = 0; i < 100; ++i) {
            promises.push(s.queryValue(["SELECT ?", [i]]));
        }
        for (let i = 0; i < promises.length; ++i) {
            expect((await promises[i]).value).toStrictEqual(i);
        }
        s.close();
    }));

    test("in microtasks", withClient(async (c) => {
        const s = c.openStream();
        const promises: Array<Promise<hrana.ValueResult>> = [];
        for (let i = 0; i < 100; ++i) {
            promises.push(s.queryValue(["SELECT ?", [i]]));
            await Promise.resolve();
        }
        for (let i = 0; i < promises.length; ++i) {
            expect((await promises[i]).value).toStrictEqual(i);
        }
        s.close();
    }));

    test("in different ticks of event loop", withClient(async (c) => {
        const s = c.openStream();
        const promises: Array<Promise<hrana.ValueResult>> = [];
        for (let i = 0; i < 100; ++i) {
            promises.push(s.queryValue(["SELECT ?", [i]]));
            await new Promise((resolve) => setTimeout(resolve, 0));
        }
        for (let i = 0; i < promises.length; ++i) {
            expect((await promises[i]).value).toStrictEqual(i);
        }
        s.close();
    }));
});

describe("Stream.close()", () => {
    test("marks the stream as closed", withClient(async (c) => {
        const s = c.openStream();
        await s.queryValue("SELECT 1");
        expect(s.closed).toStrictEqual(false);
        s.close();
        expect(s.closed).toStrictEqual(true);
    }));

    test("is idempotent", withClient(async (c) => {
        const s = c.openStream();
        await s.queryValue("SELECT 1");
        s.close();
        s.close();
    }));

    test("prevents further operations", withClient(async (c) => {
        const s = c.openStream();
        s.close();
        await expect(s.queryValue("SELECT 1")).rejects.toThrow(hrana.ClosedError);
    }));

    test("without doing anything", withClient(async (c) => {
        const s = c.openStream();
        s.close();
    }));
});

describe("Stream.closeGracefully()", () => {
    test("does not interrupt previous operations", withClient(async (c) => {
        const s = c.openStream();
        const prom = s.queryValue("SELECT 1");
        s.closeGracefully();
        expect((await prom).value).toStrictEqual(1);
    }));

    test("marks the stream as closed", withClient(async (c) => {
        const s = c.openStream();
        await s.queryValue("SELECT 1");
        expect(s.closed).toStrictEqual(false);
        s.closeGracefully();
        expect(s.closed).toStrictEqual(true);
    }));

    test("prevents further operations", withClient(async (c) => {
        const s = c.openStream();
        s.closeGracefully();
        await expect(s.queryValue("SELECT 1")).rejects.toThrow(hrana.ClosedError);
    }));

    test("without doing anything", withClient(async (c) => {
        const s = c.openStream();
        s.closeGracefully();
    }));
});

for (const useCursor of [false, true]) {
    (version >= 3 || !useCursor ? describe : describe.skip)(
        useCursor ? "batches w/ cursor" : "batches w/o cursor",
        () =>
    {
        test("empty", withClient(async (c) => {
            if (useCursor) { await c.getVersion(); }
            const s = c.openStream();
            const batch = s.batch(useCursor);
            await batch.execute();
        }));

        test("multiple statements", withClient(async (c) => {
            if (useCursor) { await c.getVersion(); }
            const s = c.openStream();

            const batch = s.batch(useCursor);
            const prom1 = batch.step().queryValue("SELECT 1");
            const prom2 = batch.step().queryRow("SELECT 'one', 'two'");
            await batch.execute();

            expect((await prom1)!.value).toStrictEqual(1);
            expect(Array.from((await prom2)!.row!)).toStrictEqual(["one", "two"]);
        }));

        test("failing statement", withClient(async (c) => {
            if (useCursor) { await c.getVersion(); }
            const s = c.openStream();

            const batch = s.batch(useCursor);
            const prom1 = batch.step().queryValue("SELECT 1");
            const prom2 = batch.step().queryValue("SELECT foobar");
            const prom3 = batch.step().queryValue("SELECT 2");
            prom2.catch(() => {}); // silence Node warning
            await batch.execute();

            expect((await prom1)!.value).toStrictEqual(1);
            await expect(prom2).rejects.toBeInstanceOf(hrana.ClientError);
            expect((await prom3)!.value).toStrictEqual(2);
        }));

        test("statement with invalid syntax", withClient(async (c) => {
            if (useCursor) { await c.getVersion(); }
            const s = c.openStream();

            const batch = s.batch(useCursor);
            const prom = batch.step().queryValue("spam");
            await expect(batch.execute().then(() => prom)).rejects.toBeInstanceOf(hrana.ClientError);
        }));

        test("ok condition", withClient(async (c) => {
            if (useCursor) { await c.getVersion(); }
            const s = c.openStream();
            const batch = s.batch(useCursor);

            const stepOk = batch.step();
            stepOk.queryValue("SELECT 1");
            const stepErr = batch.step();
            stepErr.queryValue("SELECT foospam").catch(_ => undefined);

            const prom1 = batch.step()
                .condition(hrana.BatchCond.ok(stepOk))
                .queryValue("SELECT 1");
            const prom2 = batch.step()
                .condition(hrana.BatchCond.ok(stepErr))
                .queryValue("SELECT 1");
            await batch.execute();

            expect(await prom1).toBeDefined();
            expect(await prom2).toBeUndefined();
        }));

        test("error condition", withClient(async (c) => {
            if (useCursor) { await c.getVersion(); }
            const s = c.openStream();
            const batch = s.batch(useCursor);

            const stepOk = batch.step();
            stepOk.queryValue("SELECT 1");
            const stepErr = batch.step();
            stepErr.queryValue("SELECT spameggs").catch(_ => undefined);

            const prom1 = batch.step()
                .condition(hrana.BatchCond.error(stepOk))
                .queryValue("SELECT 1");
            const prom2 = batch.step()
                .condition(hrana.BatchCond.error(stepErr))
                .queryValue("SELECT 1");
            await batch.execute();

            expect(await prom1).toBeUndefined();
            expect(await prom2).toBeDefined();
        }));

        const andOrCases = [
            {stmts: [], andOutput: true, orOutput: false},
            {stmts: ["SELECT 1"], andOutput: true, orOutput: true},
            {stmts: ["SELECT barfoo"], andOutput: false, orOutput: false},
            {stmts: ["SELECT 1", "SELECT foobaz"], andOutput: false, orOutput: true},
        ];

        const andOr: Array<{
            testName: string,
            condFun: (batch: hrana.Batch, conds: Array<hrana.BatchCond>) => hrana.BatchCond,
            expectedKey: "andOutput" | "orOutput",
        }> = [
            {testName: "and condition", condFun: hrana.BatchCond.and, expectedKey: "andOutput"},
            {testName: "or condition", condFun: hrana.BatchCond.or, expectedKey: "orOutput"},
        ];
        for (const {testName, condFun, expectedKey} of andOr) {
            test(testName, withClient(async (c) => {
                if (useCursor) { await c.getVersion(); }
                const s = c.openStream();

                for (const testCase of andOrCases) {
                    const batch = s.batch(useCursor);
                    const steps = testCase.stmts.map(stmt => {
                        const step = batch.step();
                        step.queryValue(stmt).catch(_ => undefined);
                        return step;
                    });

                    const testedCond = condFun(batch, steps.map(hrana.BatchCond.ok));
                    const prom = batch.step()
                        .condition(testedCond)
                        .queryValue("SELECT 'yes'");
                    await batch.execute();

                    expect(await prom !== undefined).toStrictEqual(testCase[expectedKey]);
                }
            }));
        }

        (version >= 3 ? describe : describe.skip)("isAutocommit condition", () => {
            test("in autocommit mode", withClient(async (c) => {
                await c.getVersion();
                const s = c.openStream();
                const batch = s.batch(useCursor);

                const prom = batch.step()
                    .condition(hrana.BatchCond.isAutocommit(batch))
                    .queryValue("SELECT 42");
                await batch.execute();

                expect((await prom)!.value).toStrictEqual(42);
            }));

            test("in transaction", withClient(async (c) => {
                await c.getVersion();
                const s = c.openStream();
                const batch = s.batch(useCursor);

                batch.step().run("BEGIN");
                const prom = batch.step()
                    .condition(hrana.BatchCond.isAutocommit(batch))
                    .queryValue("SELECT 42");
                await batch.execute();

                expect(await prom).toBeUndefined();
            }));

            test("after implicit rollback", withClient(async (c) => {
                await c.getVersion();
                const s = c.openStream();
                await s.run("DROP TABLE IF EXISTS t");
                await s.run("CREATE TABLE t (a UNIQUE)");
                await s.run("INSERT INTO t VALUES (1)");

                const batch = s.batch(useCursor);
                const prom1 = batch.step()
                    .run("INSERT OR ROLLBACK INTO t VALUES (1)");
                prom1.catch(() => {}); // silence Node warning
                const prom2 = batch.step()
                    .condition(hrana.BatchCond.not(hrana.BatchCond.isAutocommit(batch)))
                    .queryValue("SELECT 42");
                await batch.execute();

                await expect(prom1).rejects.toBeInstanceOf(hrana.ClientError);
                expect(await prom2).toBeUndefined();
            }));
        });

        test("large number of statements", withClient(async (c) => {
            if (useCursor) { await c.getVersion(); }
            const s = c.openStream();
            const batch = s.batch(useCursor);

            const proms = [];
            for (let i = 0; i < 1000; ++i) {
                proms.push(batch.step().queryValue(["SELECT 10*?", [i]]));
            }
            await batch.execute();

            for (let i = 0; i < proms.length; ++i) {
                expect((await proms[i])!.value).toStrictEqual(10*i);
            }
        }));

        test("statements that return large number of rows", withClient(async (c) => {
            if (useCursor) { await c.getVersion(); }
            const s = c.openStream();
            const batch = s.batch(useCursor);

            const proms = [];
            for (let i = 0; i < 100; ++i) {
                const sql = `
                    WITH RECURSIVE t (a) AS (SELECT 1 UNION ALL SELECT a+1 FROM t)
                    SELECT a FROM t LIMIT 10*?
                `;
                proms.push(batch.step().query([sql, [i]]));
            }
            await batch.execute();

            for (let i = 0; i < proms.length; ++i) {
                const result = (await proms[i])!;
                expect(result.rows.length).toStrictEqual(10*i);
            }
        }));

        test("not interrupted by closeGracefully()", withClient(async (c) => {
            if (useCursor) { await c.getVersion(); }
            const s = c.openStream();
            const batch = s.batch(useCursor);

            const proms = [];
            for (let i = 0; i < 100; ++i) {
                proms.push(batch.step().queryValue(["SELECT 10*?", [i]]));
            }
            const executeProm = batch.execute();
            s.closeGracefully();

            for (let i = 0; i < proms.length; ++i) {
                expect((await proms[i])!.value).toStrictEqual(10*i);
            }
            await executeProm;
        }));

        test("batches are executed sequentially", withClient(async (c) => {
            if (useCursor) { await c.getVersion(); }
            const s = c.openStream();
            await s.run("DROP TABLE IF EXISTS t");
            await s.run("CREATE TABLE t (a)");
            await s.run("INSERT INTO t VALUES (0)");

            const updateProms = [];
            const batchProms = [];
            for (let i = 0; i < 100; ++i) {
                const batch = s.batch(useCursor);
                for (let j = 0; j < 20; ++j) {
                    updateProms.push(batch.step().queryValue(
                        "UPDATE t SET a = a + 1 RETURNING a",
                    ));
                }
                batchProms.push(batch.execute());
            }

            for (const batchProm of batchProms) {
                await batchProm;
            }
            for (let k = 0; k < updateProms.length; ++k) {
                expect((await updateProms[k])!.value).toStrictEqual(k + 1);
            }
        }));
    });
}

(version >= 2 ? describe : describe.skip)("describe()", () => {
    test("trivial statement", withClient(async (c) => {
        await c.getVersion();
        const s = c.openStream();
        const d = await s.describe("SELECT 1 AS one");
        expect(d.paramNames).toStrictEqual([]);
        expect(d.columns).toStrictEqual([
            {name: "one", decltype: undefined},
        ]);
    }));

    test("param names", withClient(async (c) => {
        await c.getVersion();
        const s = c.openStream();
        const d = await s.describe("SELECT ?, ?3, :one, $two, @three");
        expect(d.paramNames).toStrictEqual([undefined, undefined, "?3", ":one", "$two", "@three"]);
    }));

    test("columns", withClient(async (c) => {
        await c.getVersion();
        const s = c.openStream();
        await s.run("BEGIN");
        await s.run("DROP TABLE IF EXISTS t");
        await s.run("CREATE TABLE t (a TEXT, b int NOT NULL, c FURRY BUNNY)");
        const d = await s.describe("SELECT a, b, c, a + b AS sum FROM t");
        expect(d.columns).toStrictEqual([
            {name: "a", decltype: "TEXT"},
            {name: "b", decltype: "INT"},
            {name: "c", decltype: "FURRY BUNNY"},
            {name: "sum", decltype: undefined},
        ]);
    }));

    test("isExplain", withClient(async (c) => {
        await c.getVersion();
        const s = c.openStream();
        expect((await s.describe("SELECT 1")).isExplain).toStrictEqual(false);
        expect((await s.describe("EXPLAIN SELECT 1")).isExplain).toStrictEqual(true);
        expect((await s.describe("EXPLAIN QUERY PLAN SELECT 1")).isExplain).toStrictEqual(true);
    }));

    test("isReadonly", withClient(async (c) => {
        await c.getVersion();
        const s = c.openStream();
        await s.run("BEGIN");
        await s.run("DROP TABLE IF EXISTS t");
        await s.run("CREATE TABLE t (a TEXT)");
        expect((await s.describe("SELECT 1")).isReadonly).toStrictEqual(true);
        expect((await s.describe("UPDATE t SET a = 'foo'")).isReadonly).toStrictEqual(false);
        expect((await s.describe("DROP TABLE t")).isReadonly).toStrictEqual(false);
        expect((await s.describe("COMMIT")).isReadonly).toStrictEqual(true);
    }));

    (isWs ? test : test.skip)("without calling getVersion() first", withWsClient(async (c) => {
        const s = c.openStream();
        expect(() => s.describe("SELECT 1")).toThrow(hrana.ProtocolVersionError);
    }));
});

(version >= 2 ? describe : describe.skip)("sequence()", () => {
    test("no statements", withClient(async (c) => {
        await c.getVersion();
        const s = c.openStream();
        await s.sequence("  \n-- this is a comment\n");
    }));

    test("a single statement", withClient(async (c) => {
        await c.getVersion();
        const s = c.openStream();
        await s.run("BEGIN");
        await s.run("DROP TABLE IF EXISTS t");
        await s.sequence("CREATE TABLE t(a);");
        expect((await s.queryValue("SELECT COUNT(*) FROM t")).value).toStrictEqual(0);
    }));

    test("multiple statements", withClient(async (c) => {
        await c.getVersion();
        const s = c.openStream();
        await s.sequence(`
            BEGIN;
            DROP TABLE IF EXISTS t;
            CREATE TABLE t(a);
            INSERT INTO t VALUES (1), (2), (3);
        `);
        expect((await s.queryValue("SELECT COUNT(*) FROM t")).value).toStrictEqual(3);
    }));
});

(version >= 2 ? describe : describe.skip)("storeSql()", () => {
    function withSqlOwner(f: (s: hrana.Stream, owner: hrana.SqlOwner) => Promise<void>): () => Promise<void> {
        return async () => {
            if (isWs) {
                const client = hrana.openWs(url, jwt, 3);
                try {
                    await client.getVersion();
                    const stream = client.openStream();
                    await f(stream, client);
                } finally {
                    client.close();
                }
            } else if (isHttp) {
                const client = hrana.openHttp(url, jwt, undefined, 3);
                try {
                    const stream = client.openStream();
                    await f(stream, stream);
                } finally {
                    client.close();
                }
            } else {
                throw new Error("expected either ws or http URL");
            }
        };
    }

    test("query", withSqlOwner(async (s, owner) => {
        const sql = owner.storeSql("SELECT 42");
        expect((await s.queryValue(sql)).value).toStrictEqual(42);
    }));

    test("query with args", withSqlOwner(async (s, owner) => {
        const sql = owner.storeSql("SELECT ?");
        expect((await s.queryValue([sql, [42]])).value).toStrictEqual(42);
    }));

    for (const useCursor of [false, true]) {
        (version >= 3 || !useCursor ? test : test.skip)(
            useCursor ? "batch w/ cursor" : "batch w/o cursor",
            withSqlOwner(async (s, owner) =>
        {
            const sql1 = owner.storeSql("SELECT 11");
            const sql2 = owner.storeSql("SELECT 'one', 'two'");
            const batch = s.batch();
            const prom1 = batch.step().queryValue(sql1);
            const prom2 = batch.step().queryRow(sql2);
            await batch.execute();

            expect((await prom1)!.value).toStrictEqual(11);
            expect(Array.from((await prom2)!.row!)).toStrictEqual(["one", "two"]);
        }));
    }

    test("describe", withSqlOwner(async (s, owner) => {
        const sql = owner.storeSql("SELECT :a, $b");
        const d = await s.describe(sql);
        expect(d.paramNames).toStrictEqual([":a", "$b"]);
    }));

    test("close", withSqlOwner(async (s, owner) => {
        const sql = owner.storeSql("SELECT :a, $b");
        expect(sql.closed).toBe(false);
        sql.close();
        expect(sql.closed).toBe(true);
    }));

    (isWs ? test : test.skip)("without calling getVersion() first", withWsClient(async (c) => {
        expect(() => c.storeSql("SELECT 1")).toThrow(hrana.ProtocolVersionError);
    }));

    (isHttp ? test : test.skip)("using SQL stored on another stream", withHttpClient(async (c) => {
        const s1 = c.openStream();
        const s2 = c.openStream();

        const sql = s1.storeSql("SELECT 1");
        expect(() => s2.queryValue(sql)).toThrow(/SQL text/);
    }));
});

test("getVersion()", withClient(async (c) => {
    expect(await c.getVersion()).toBeGreaterThanOrEqual(version);
}));

(version >= 3 ? test : test.skip)("getAutocommit()", withClient(async (c) => {
    await c.getVersion();
    const s = c.openStream();
    expect(await s.getAutocommit()).toStrictEqual(true);

    await s.run("DROP TABLE IF EXISTS t");
    await s.run("CREATE TABLE t (a UNIQUE)");
    expect(await s.getAutocommit()).toStrictEqual(true);

    await s.run("BEGIN");
    expect(await s.getAutocommit()).toStrictEqual(false);

    await s.run("INSERT INTO t VALUES (1)");
    expect(await s.getAutocommit()).toStrictEqual(false);

    await expect(s.run("INSERT OR ROLLBACK INTO t VALUES (1)"))
        .rejects.toBeInstanceOf(hrana.ClientError);
    expect(await s.getAutocommit()).toStrictEqual(true);
}));

(isHttp ? describe : describe.skip)("customFetch", () => {
    test("custom function is called", async () => {
        let fetchCalledCount = 0;

        async function customFetch(this: unknown, request: Request): Promise<Response> {
            expect(request).toBeInstanceOf(Request);
            expect(this === undefined || this === globalThis).toBe(true);
            expect(request.url.startsWith(url)).toBe(true);

            fetchCalledCount += 1;
            return fetch(request);
        }

        const c = hrana.openHttp(url, jwt, customFetch, 3);
        try {
            const s = c.openStream();
            const res = await s.queryValue("SELECT 1");
            expect(res.value).toStrictEqual(1);

            expect(fetchCalledCount).toBeGreaterThan(0);
        } finally {
            c.close();
        }
    });

    test("exception thrown from the custom function", async () => {
        function customFetch() {
            throw new Error("testing exception thrown from customFetch()");
        }

        const c = hrana.openHttp(url, jwt, customFetch, 3);
        try {
            const s = c.openStream();
            await expect(s.queryValue("SELECT 1")).rejects
                .toThrow("testing exception thrown from customFetch()");
        } finally {
            c.close();
        }
    });

    test("rejected promise returned from the custom function", async () => {
        function customFetch(): Promise<Response> {
            return Promise.reject(new Error("testing rejection returned from customFetch()"));
        }

        const c = hrana.openHttp(url, jwt, customFetch, 3);
        try {
            const s = c.openStream();
            await expect(s.queryValue("SELECT 1")).rejects
                .toThrow("testing rejection returned from customFetch()");
        } finally {
            c.close();
        }
    });
});
