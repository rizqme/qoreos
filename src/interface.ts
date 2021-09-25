import { Database, Subspace, Transaction, TupleItem } from 'foundationdb';

export interface JsonObject {
    [member: string]: Json;
}
export interface JsonArray extends Array<Json> {}

export type JsonPrimitive = string | number | boolean | null;

export type Json = JsonPrimitive | Object | JsonArray;

export type RowId = number;

export type IterOption = {
    limit?: number;
    cursor?: JsonPrimitive;
};

export type IterResult<C, T> = {
    cursor: C;
    results: T[];
};

export type FindOp = "eq" | "gte" | "gt" | "lte" | "lt";

export type Db = Database<TupleItem[], TupleItem[], any, any>;
export type Tuple = TupleItem[];
export type Space = Subspace<TupleItem[], TupleItem[], any, any>;
export type Tx = Transaction<TupleItem[], TupleItem[], any, any>;