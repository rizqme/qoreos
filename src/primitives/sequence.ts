import { encoders, keySelector, TupleItem } from 'foundationdb';
import { Space, Tuple, Tx } from '../interface';
import { COUNT, ENDNUM, ENDSTAMP, IDS, INDEX } from '../namespace';
import { baseToStamp, Stamp, issueStamp, stampToBase } from '../stamp';

export class Sequence {

    private readonly tx: Tx;
    private readonly snapshot: Tx;
    private readonly space: Space;
    private readonly index: Space;
    private readonly ids: Space;

    constructor(tx: Tx, snapshot: Tx, space: Tuple) {
        this.space = tx.subspace.at(space);
        this.ids = this.space.at([IDS]).withValueEncoding(encoders.tuple);
        this.index = this.space.at([INDEX]);
        this.tx = tx.at(this.space);
        this.snapshot = snapshot.at(this.space);
    }

    async insert(id: string) {
        this.tx.at(this.index).setVersionstampedKey([issueStamp()], id);
        this.tx.at(this.ids).setVersionstampedValue([id], [issueStamp()]);
    }

    async getLastIndex() {
        let key = await this.snapshot.at(this.index).getKey(keySelector.lastLessThan([ENDSTAMP]));
        if (!key) return null;
        const stamp = key[key.length-1] as Stamp;
        return stampToBase(stamp);
    }

    async getId(index: number): Promise<string> {
        return this.tx.at(this.index).get([index]);
    }

    async getIndex(id: string) {
        let [stamp] = await this.snapshot.at(this.ids).get([id]);
        if (!stamp) return null;
        return stampToBase(stamp);
    }

    async remove(id: string) {
        let index = await this.getIndex(id);
        if (!index) return;
        this.tx.at(this.ids).delete([id]);
        this.tx.at(this.index).delete([baseToStamp(index)]);
    }

    async list(limit = 50, cursor?: number) {
        let count = 0;
        let result: { index: string; id: string }[] = [];
        for await (const {id, index} of this.iterate(cursor)) {
            result.push({id, index});
            if (++count >= limit) break;
        }

        return result;
    }

    async *iterate(cursor?: number) {
        let cur = cursor || 0;
        for await (const [key, id] of this.snapshot.at(this.index).getRange(
            keySelector.firstGreaterThan([cur]),
            [ENDSTAMP]
        )) {
            yield {id: id as string, index: key[0] as string};
        }
    }
}