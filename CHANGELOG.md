# Changelog

## Unreleased

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
