

import { Json, Space, Tuple, Tx } from '../interface';
import { START, END, OBJECTS, VERSIONS, STARTSTAMP, LASTUPDATE, DATA } from '../namespace';
import { encoders, keySelector, TupleItem} from 'foundationdb';
import { baseToStamp, commitStamp, issueStamp, Stamp, stampCommitBase, stampToBase } from '../stamp';
import { versions } from 'process';

export class VMap<T extends TupleItem, K extends Json> {

    protected readonly tx: Tx;
    protected readonly snapshot: Tx;
    protected readonly space: Space;
    protected readonly objects: Space;
    protected readonly versions: Space;

    constructor(tx: Tx, snapshot: Tx, space: Tuple){
        this.space = tx.subspace.at(space);
        this.tx = tx.at(this.space);
        this.snapshot = snapshot.at(this.space);
        this.objects = this.space.at([OBJECTS]).withValueEncoding(encoders.tuple);
        this.versions = this.space.at([VERSIONS]);
    }



    set(key: T, value: K) {
        this.tx.at(this.versions).setVersionstampedKey([key, issueStamp()], value);
        this.tx.at(this.objects).setVersionstampedValue([key], [issueStamp(), value]);
    }

    remove(key: T) {
        this.tx.at(this.versions).setVersionstampedKey([key, issueStamp()], undefined);
        this.tx.at(this.objects).setVersionstampedValue([key], [issueStamp()]);
    }

    async get(key: T): Promise<K> {
        let [val] = await this.tx.at(this.objects).get([key]);
        return val;
    }

    async getkv(key: T) {
        let [stamp] = await this.snapshot.at(this.objects).get([key]);
        if (!stamp) return null;
        return stampCommitBase(stamp);
    }

    async getv(key: T, version: string) {
        const stamp = commitStamp(baseToStamp(version), true);
        let res = await this.snapshot.at(this.versions).getRangeAll([key, STARTSTAMP], [key, stamp], {limit:1, reverse: true});
        if (!res.length) return null;
        return res[0][1] as K;
    }

    async clearv(key: T, version: string) {
        this.tx.at(this.versions).clearRange([key, STARTSTAMP],[key, commitStamp(baseToStamp(version), true)]);
    }

    async list(limit = 50, cursor = '') {
        let count = 0;
        let result: {key:T, value: K}[] = [];
        for await (const item of this.iterate(cursor)) {
            result.push(item);
            if (++count >= limit) break;
        }
        return result;
    }

    async listv(version: string, limit = 50, cursor = '') {
        let count = 0;
        let result: {key:T, value: K}[] = [];
        for await (const item of this.iteratev(version, cursor)) {
            result.push(item);
            if (++count >= limit) break;
        }
        return result;
    }

    async *iterate(cursor = START) {
        let tx = this.tx.at(this.objects);
        for await (const [[key], [, value]] of tx.getRange(
            keySelector.firstGreaterThan([cursor]),
            [END]
        ) as AsyncGenerator<[[T], [Stamp,K]]>) {
            if (value === undefined) continue;
            yield { key, value };
        }
    }

    async *iteratev(version: string, cursor = START) {
        version = stampCommitBase(version);
        let tx = this.snapshot.at(this.objects);
        for await (let [[key], [stamp, value]] of tx.getRange(
            keySelector.firstGreaterThan([cursor]),
            [END]
        ) as AsyncGenerator<[[T], [Stamp, K]]>) {
            let ver = stampToBase(commitStamp(stamp));
            if (version < ver) {
                value = await this.getv(key, version);
            }
            if (value === undefined) continue;
            yield { key, value };
        }
    }
}
