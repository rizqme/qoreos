
import {Stamp} from "./stamp";

export const
ENTITIES = 1,
TABLES = 2,
INFO = 3,
TYPE = 4,
COUNT = 5,
ORDER = 6,
OBJECTS = 7,
DELETED = 8,
FIELDS = 9,
VERSIONS = 10,
IDS = 11,
NAMES = 12,
ROWS = 13,
SUBS = 14,
INDEX = 15,
EVENTS = 16,
CURSOR = 17,
LASTUPDATE = 18,
LOG = 19,
RELS = 20,
DATA = 21,
START = "",
STARTSTAMP = {type: "versionstamp", value: Buffer.alloc(12)} as Stamp,
END = "\xFF",
ENDNUM = Number.MAX_SAFE_INTEGER,
ENDSTAMP = {type: "versionstamp", value: Buffer.from("////////////////", "base64")} as Stamp;