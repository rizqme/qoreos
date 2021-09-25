import { FindOp, IterResult, Json, Space, Tuple, Tx } from '../interface';
import { END, START } from '../namespace';
import { keySelector, TupleItem } from 'foundationdb';



function selectStart(start: Tuple, exclStart: boolean) {
    return exclStart ? keySelector.firstGreaterThan(start) : start;
}

function selectEnd(end: Tuple, exclEnd: boolean) {
    return exclEnd ? end : keySelector.next(keySelector.from(end));
}

export class Multimap<T extends TupleItem, K extends TupleItem> {

    private readonly tx: Tx;
    private readonly space: Space;

    constructor(tx: Tx, space: Tuple, private start: T, private end: T) {
        this.space = tx.subspace.at(space);
        this.tx = tx.at(this.space);
    }

    set(key: T, value: K) {
        return this.tx.set([key, value], value);
    }

    remove(key: T, value: K) {
        return this.tx.delete([key, value]);
    }
    
    async rangeAll(config?: {start?: T, end?: T, exclStart?: boolean, exclEnd?: boolean, cursor?: [T, string], limit?: number}) {
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

    async *range(config?: {start?: T, end?: T, exclStart?: boolean, exclEnd?: boolean, cursor?: [T, string]}) {
        config = {start: this.start, end: this.end,  ...config}
        for await (const [keys, value] of this.tx.getRange(
            config.cursor ? keySelector.firstGreaterThan(config.cursor) : selectStart([config.start], config.exclStart), 
            selectEnd([config.end], config.exclEnd))) {
            let key = keys[0] as T;
            yield {key, value: value as K};
        }
    }

    async list(key: T, limit = 50, cursor = START) {
        let count = 0;
        let result: string[] = [];
        for await (const value of this.iterate(key, cursor)) {
            result.push(value);
            if (++count >= limit) break;
        }
        return result;
    }
    
    async *iterate(key: T, cursor = START) {
        for await (const [, id] of this.tx.getRange(
            keySelector.firstGreaterThan([key, cursor]),
            [key, END]
        )) {
            yield id as string;
        }
    }
}