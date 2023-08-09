import { InternalError } from "./errors.js";

export function impossible(value: never, message: string): Error {
    throw new InternalError(message);
}
