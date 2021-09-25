import exp from "constants";


export type Stamp = {
    type: "versionstamp";
    value: Buffer;
}

export type UnboundStamp = {
    type: "unbound versionstamp";
    code?: number;
}

export function issueStamp(code?: number): UnboundStamp {
    return {
        type: "unbound versionstamp",
        code
    }
}

export function stampFromBuffer(buf: Buffer): Stamp {
    let stampBuf = Buffer.alloc(12);
    buf.copy(stampBuf);
    return {
        type: "versionstamp",
        value: stampBuf
    };
}

export function stampCommitBase(base: string) {
    return stampToBase(commitStamp({
        type: "versionstamp",
        value: base64ToBuf(base)
    }));
}

export function commitStamp(stamp: Stamp, end = false): Stamp {
    const buf = Buffer.alloc(12);
    stamp.value.copy(buf);
    if (end) {
        buf[10] = 0xFF;
        buf[11] = 0xFF;
    } else { 
        buf[10] = 0;
        buf[11] = 0;
    }
    return {
        type: "versionstamp",
        value: buf
    };
}

export function stampToBase(stamp: Stamp) {
    return stamp.value.toString("base64");
}

export function baseToStamp(base: string, commit = false): Stamp {
    return {
        type: "versionstamp",
        value: base64ToBuf(base)
    };
}

export function base64ToBuf(b64: string, len = 12) {
    let b64Buf = Buffer.from(b64 || "", "base64");
    let buf = Buffer.alloc(len);
    if (buf.length < b64Buf.length) throw new Error(`base64 string larger than allocated buffer`);
    b64Buf.copy(buf, buf.length - b64Buf.length);
    return buf;
}