export function impossible(value: never, message: string): Error {
    throw new Error(message);
}
