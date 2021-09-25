import { FindOp, IterResult, Json, Space, Tuple, Tx } from '../interface';
import { DATA, END, OBJECTS, START, VERSIONS } from '../namespace';
import { encoders, keySelector, TupleItem } from 'foundationdb';
import { commitStamp, issueStamp, Stamp, stampCommitBase, stampToBase } from '../stamp';



function selectStart(start: Tuple, exclStart: boolean) {
    return exclStart ? keySelector.firstGreaterThan(start) : start;
}

function selectEnd(end: Tuple, exclEnd: boolean) {
    return exclEnd ? end : keySelector.next(keySelector.from(end));
}

export class VMultimap<T extends TupleItem, K extends TupleItem> {

    protected readonly tx: Tx;
    protected readonly snapshot: Tx;
    protected readonly space: Space;
    protected readonly objects: Space;

    constructor(tx: Tx, snapshot: Tx, space: Tuple, private start: T, private end: T) {
        this.space = tx.subspace.at(space);
        this.tx = tx.at(this.space);
        this.snapshot = snapshot.at(this.space);
        this.objects = this.space.at([OBJECTS]).withValueEncoding(encoders.tuple);
    }

    set(key: T, value: K) {
        this.tx.at(this.objects).setVersionstampedKey([key, value, issueStamp()], true);
    }

    remove(key: T, value: K) {
        this.tx.at(this.objects).setVersionstampedKey([key, value, issueStamp()], false);
    }
    
    async rangeAll(config?: {start?: T, end?: T, exclStart?: boolean, exclEnd?: boolean, cursor?: [T, string], version?: string, limit?: number}) {
        config = {start: this.start, end: this.end,  ...config}
        let count = 0;
        let result: {key: T, value: K}[] = [];
        let limit = config.limit || 50;
        for await (const {key, value} of this.range(config)) {
            result.push({key, value});
            if (++count >= limit) break;
        }
        return result;
    }

    async *range(config?: {start?: T, end?: T, exclStart?: boolean, exclEnd?: boolean, cursor?: [T, string], version?: string}) {
        config = {start: this.start, end: this.end, ...config}
        let lastVal: K;
        for await (const [[k, v, s], t] of this.tx.getRange(
            config.cursor ? keySelector.firstGreaterThan(config.cursor) : selectStart([config.start], config.exclStart), 
            selectEnd([config.end], config.exclEnd)
        ) as AsyncGenerator<[[T, K, Stamp], boolean]> ) {
            let ver = stampToBase(commitStamp(s));
            if (v === lastVal) continue;
            if (!config.version || config.version >= ver) {
                lastVal = v;
                if (t) yield {key:k, value:v};
            }
        }
    }

    async list(key: T, limit = 50, cursor = START) {
        let count = 0;
        let result: K[] = [];
        for await (const value of this.iterate(key, cursor)) {
            result.push(value);
            if (++count >= limit) break;
        }
        return result;
    }
    
    iterate(key: T, cursor = START) {
        return this.iteratev(key, null, cursor);
    }

    async *iteratev(key: T, version?: string, cursor = START) {
        if (version)
            version = stampCommitBase(version);
        let tx = this.snapshot.at(this.objects);
        let lastVal: K;
        for await (let [[k, v, s], t] of tx.getRange(
            keySelector.firstGreaterThan([key, cursor]),
            [key, this.end],
            {reverse: true}
        ) as AsyncGenerator<[[T, K, Stamp], boolean]>) {
            let ver = stampToBase(commitStamp(s));
            if (v === lastVal) continue;
            if (!version || version >= ver) {
                lastVal = v;
                if (t) yield v;
            }
        }
    }
}