import { LibsqlUrlParseError } from "./errors.js";

/** Result of parsing a libsql URL (using {@link parseLibsqlUrl()}). */
export type ParsedLibsqlUrl = {
    /** The URL which can be passed to {@link open()} to open a {@link Client}. */
    hranaUrl: string,
    /** The optional `authToken` query parameter that should be passed as `jwt` to {@link open()}. */
    authToken: string | undefined,
};

/** Parses a URL compatible with the libsql client (`@libsql/client`). This URL may have the "libsql:" scheme
 * and may contain query parameters. */
export function parseLibsqlUrl(urlStr: string): ParsedLibsqlUrl {
    const url = new URL(urlStr);

    let authToken: string | undefined = undefined;
    for (const [key, value] of url.searchParams.entries()) {
        if (key === "authToken") {
            authToken = value;
        } else {
            throw new LibsqlUrlParseError(`Unknown URL query argument ${JSON.stringify(key)}`);
        }
    }

    let hranaScheme: string;
    if (url.protocol === "http:") {
        throw new LibsqlUrlParseError(
            'This client does not support "http:" URLs. Please use a "ws:" URL instead.'
        );
    } else if (url.protocol === "https:") {
        throw new LibsqlUrlParseError(
            'This client does not support "https:" URLs. Please use a "wss:" URL instead.'
        );
    } else if (url.protocol === "libsql:") {
        hranaScheme = "wss:";
    } else if (url.protocol === "ws:" || url.protocol === "wss:") {
        hranaScheme = url.protocol;
    } else {
        throw new LibsqlUrlParseError(
            `This client does not support ${JSON.stringify(url.protocol)} URLs. ` +
                'Please use a "libsql:", "ws:" or "wss:" URL instead.'
        );
    }

    if (url.username || url.password) {
        throw new LibsqlUrlParseError(
            "This client does not support HTTP Basic authentication with a username and password. " +
                'You can authenticate using a token passed in the "authToken" URL query parameter.',
        );
    }
    if (url.hash) {
        throw new LibsqlUrlParseError("URL fragments are not supported");
    }

    let hranaPath = url.pathname;
    if (hranaPath === "/") {
        hranaPath = "";
    }

    const hranaUrl = `${hranaScheme}//${url.host}${hranaPath}`;
    return { hranaUrl, authToken };
}
