import * as hrana from "..";

describe("parseLibsqlUrl()", () => {
    function expectParse(url: string, parsed: Partial<hrana.ParsedLibsqlUrl>) {
        parsed.authToken = parsed.authToken ?? undefined;
        expect(hrana.parseLibsqlUrl(url)).toStrictEqual(parsed);
    }

    function expectParseError(url: string, message: RegExp) {
        expect(() => hrana.parseLibsqlUrl(url)).toThrow(message);
    }

    test("ws/wss URL", () => {
        expectParse("ws://localhost", {hranaUrl: "ws://localhost"});
        expectParse("ws://localhost:8080", {hranaUrl: "ws://localhost:8080"});
        expectParse("ws://127.0.0.1:8080", {hranaUrl: "ws://127.0.0.1:8080"});
        expectParse("ws://[2001:db8::1]:8080", {hranaUrl: "ws://[2001:db8::1]:8080"});
        expectParse("wss://localhost", {hranaUrl: "wss://localhost"});
    });

    test("libsql URL", () => {
        expectParse("libsql://localhost", {hranaUrl: "wss://localhost"});
        expectParse("libsql://localhost:8080", {hranaUrl: "wss://localhost:8080"});
    });

    test("authToken in query params", () => {
        expectParse("wss://localhost?authToken=foobar", {
            hranaUrl: "wss://localhost",
            authToken: "foobar",
        });
    });

    test("unknown query param", () => {
        expectParseError("ws://localhost?foo", /"foo"/);
    });

    test("http/https scheme", () => {
        expectParseError("http://localhost", /"http:".*"ws:"/);
        expectParseError("https://localhost", /"https:".*"wss:"/);
    });

    test("unknown scheme", () => {
        expectParseError("spam://localhost", /"spam:"/);
    });

    test("basic auth", () => {
        expectParseError("ws://alice@localhost", /Basic/);
        expectParseError("ws://alice:password@localhost", /Basic/);
    });

    test("fragment", () => {
        expectParseError("ws://localhost#eggs", /fragments/);
    });
});
