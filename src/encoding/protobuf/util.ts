export type WireType = 0 | 1 | 2 | 3 | 4 | 5;
export const VARINT: WireType = 0;
export const FIXED_64: WireType = 1;
export const LENGTH_DELIMITED: WireType = 2;
export const GROUP_START: WireType = 3;
export const GROUP_END: WireType = 4;
export const FIXED_32: WireType = 5;
