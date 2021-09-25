import { encoders } from 'foundationdb';
import { counterEncoding } from 'foundationdb/dist/lib/directory';
import { int32ToBuf } from '../utils';
import { Tuple, Tx } from '../interface';

export class Counter {

    private readonly tx: Tx;

    constructor(
        tx: Tx,
        public readonly space: Tuple
    ){
        this.tx = tx.at(tx.subspace.at(space).withValueEncoding(encoders.buf));
    }

    increment(num = 1) {
        this.tx.add([], int32ToBuf(num));

    }

    decrement(num = 1) {
        return this.increment(-num);
    }

    incrementAndGet(num = 1) {
        this.increment(num);
        return this.get();
    }

    async get(): Promise<number> {
        let val = await this.tx.get([]);
        return val ? val.readInt32LE() : 0;
    }

    set(value: number) {
        this.tx.set([], int32ToBuf(value));
    }
    reset() {
        this.set(0);
    }
}
