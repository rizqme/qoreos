
import { Json, Space, Tuple, Tx } from '../interface';
import { START, END, OBJECTS } from '../namespace';
import { keySelector } from 'foundationdb';

export class Map<T extends Json> {

    protected readonly tx: Tx;
    protected readonly space: Space;

    constructor(tx: Tx, space: Tuple){
        this.space = tx.subspace.at(space);
        this.tx = tx.at(this.space);
    }

    set(key: string, value: T) {
        return this.tx.set([OBJECTS, key], value);
    }

    remove(key: string) {
        return this.tx.delete([OBJECTS, key]);
    }

    get(key: string) {
        return this.tx.get([OBJECTS, key]);
    }

    async list(limit = 50, cursor = '') {
        let count = 0;
        let result: {key:string, value: T}[] = [];
        for await (const item of this.iterate(cursor)) {
            result.push(item);
            if (++count >= limit) break;
        }
        return result;
    }

    async *iterate(cursor = START) {
        let tx = this.tx.at(this.space.at([OBJECTS]));
        for await (const [keys, val] of tx.getRange(
            keySelector.firstGreaterThan([cursor]),
            [END]
        )) {
            let key = keys[keys.length - 1] as string;
            let value = val as T;
            yield { key, value };
        }
    }
}
