import * as hrana from "..";

describe("parseLibsqlUrl()", () => {
    function expectParse(url: string, parsed: Partial<hrana.ParsedLibsqlUrl>) {
        parsed.hranaWsUrl ??= undefined;
        parsed.hranaHttpUrl ??= undefined;
        parsed.authToken ??= undefined;
        expect(hrana.parseLibsqlUrl(url)).toStrictEqual(parsed);
    }

    function expectParseError(url: string, message: RegExp) {
        expect(() => hrana.parseLibsqlUrl(url)).toThrow(message);
    }

    test("ws/wss URL", () => {
        expectParse("ws://localhost", {hranaWsUrl: "ws://localhost"});
        expectParse("ws://localhost:8080", {hranaWsUrl: "ws://localhost:8080"});
        expectParse("ws://127.0.0.1:8080", {hranaWsUrl: "ws://127.0.0.1:8080"});
        expectParse("ws://[2001:db8::1]:8080", {hranaWsUrl: "ws://[2001:db8::1]:8080"});
        expectParse("ws://localhost/some/path", {hranaWsUrl: "ws://localhost/some/path"});
        expectParse("wss://localhost", {hranaWsUrl: "wss://localhost"});
    });

    test("http/https URL", () => {
        expectParse("http://localhost", {hranaHttpUrl: "http://localhost"});
        expectParse("http://localhost/some/path", {hranaHttpUrl: "http://localhost/some/path"});
        expectParse("https://localhost", {hranaHttpUrl: "https://localhost"});
    });

    test("libsql URL", () => {
        expectParse("libsql://localhost", {
            hranaWsUrl: "wss://localhost",
            hranaHttpUrl: "https://localhost",
        });
        expectParse("libsql://localhost:8080", {
            hranaWsUrl: "wss://localhost:8080",
            hranaHttpUrl: "https://localhost:8080",
        });
        expectParse("libsql://localhost/some/path", {
            hranaWsUrl: "wss://localhost/some/path",
            hranaHttpUrl: "https://localhost/some/path",
        });
    });

    test("tls disabled", () => {
        expectParse("ws://localhost?tls=0", {hranaWsUrl: "ws://localhost"});
        expectParse("http://localhost?tls=0", {hranaHttpUrl: "http://localhost"});
        expectParseError("wss://localhost?tls=0", /tls=0/);
        expectParseError("https://localhost?tls=0", /tls=0/);
        expectParse("libsql://localhost:8080?tls=0", {
            hranaWsUrl: "ws://localhost:8080",
            hranaHttpUrl: "http://localhost:8080",
        });
        expectParseError("libsql://localhost?tls=0", /tls=0.* explicit port/);
    });

    test("tls enabled", () => {
        expectParse("wss://localhost?tls=1", {hranaWsUrl: "wss://localhost"});
        expectParse("https://localhost?tls=1", {hranaHttpUrl: "https://localhost"});
        expectParseError("ws://localhost?tls=1", /tls=1/);
        expectParseError("http://localhost?tls=1", /tls=1/);
        expectParse("libsql://localhost?tls=1", {
            hranaWsUrl: "wss://localhost",
            hranaHttpUrl: "https://localhost",
        });
    });

    test("invalid value for tls", () => {
        expectParseError("ws://localhost?tls=yes", /"tls"/);
    });

    test("authToken in query params", () => {
        expectParse("wss://localhost?authToken=foobar", {
            hranaWsUrl: "wss://localhost",
            authToken: "foobar",
        });
    });

    test("unknown query param", () => {
        expectParseError("ws://localhost?foo", /"foo"/);
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
