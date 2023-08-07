import * as hrana from "@libsql/hrana-client";

const client = hrana.openWs(process.env.URL ?? "ws://localhost:8080", process.env.JWT);
const stream = client.openStream();
console.log(await stream.queryValue("SELECT 1"));
client.close();
