# Changelog

## Unreleased

- Update `isomorphic-fetch` dependency for built-in Node fetch(). This package now requires Node 18 or later.

## 0.5.2 -- 2023-09-11

- Switch to use Hrana 2 by default to let Hrana 3 cook a bit longer.

## 0.5.1 -- 2023-09-11

- Update `isomorphic-{fetch, ws}` dependencies for Bun support.

## 0.5.0 -- 2023-07-29

- **Added support for Hrana 3**, which included some API changes:
    - Added variant `3` to the `ProtocolVersion` type
    - Added `BatchCond.isAutocommit()`
    - Added `Stream.getAutocommit()`
    - Added parameter `useCursor` to `Stream.batch()`
- **Changed meaning of `Stream.close()`**, which now closes the stream immediately
    - Added `Stream.closeGracefully()`
- Changed type of `StmtResult.lastInsertRowid` to bigint
- Changed `BatchCond.and()` and `BatchCond.or()` to pass the `Batch` object
- Added `Stream.client()`
- Added `MisuseError` and `InternalError`
- Added reexport of `WebSocket` from `@libsql/isomorphic-ws`
- Added reexports of `fetch`, `Request`, `Response` and other types from `@libsql/isomorphic-fetch`
- Dropped workarounds for broken WebSocket support in Miniflare 2

## 0.4.4 -- 2023-08-15

- Pass a `string` instead of `URL` to the `Request` constructor

## 0.4.3 -- 2023-07-18

- Added `customFetch` argument to `openHttp()` to override the `fetch()` function

## 0.4.2 -- 2023-06-22

- Added `IntMode`, `Client.intMode` and `Stream.intMode`

## 0.4.1 -- 2023-06-12

- Fixed environments that don't support `queueMicrotask()` by implementing a ponyfill [libsql-client-ts#47](https://github.com/libsql/libsql-client-ts/issues/47)

## 0.4.0 -- 2023-06-07

- **Added support for Hrana over HTTP**, which included some API changes:
    - Removed `open()`, replaced with `openHttp()` and `openWs()`
    - Added `SqlOwner` interface for the `storeSql()` method, which is implemented by `WsClient` and `HttpStream`
- Added HTTP `status` to `HttpServerError`
- Changed `parseLibsqlUrl()` to support both WebSocket and HTTP URLs
- Changed `Value` type to include `bigint`
