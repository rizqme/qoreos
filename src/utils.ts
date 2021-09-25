

export function int32ToBuf(num: number): Buffer {
    const buf = Buffer.alloc(4);
    buf.writeInt32LE(num);
    return buf;
}
