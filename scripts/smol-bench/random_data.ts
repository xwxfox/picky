const randomInt = (min: number, max: number) => Math.floor(Math.random() * (max - min)) + min;

type LargeItem = {
    active: boolean;
    created: Date | string;
    flags: Array<string>;
    id: number;
    Logs: Array<{ tags: Array<string>; type: string; when: Date | string }>;
    meta: {
        owner: {
            name: string;
            nickname?: string | null;
        };
    };
    name: string | null;
    score: number;
};

const mulberry32 = (seed: number) => {
    let t = seed;
    return () => {
        t += 0x6D_2B_79_F5;
        let value = t;
        value = Math.imul(value ^ (value >>> 15), value | 1);
        value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
        return ((value ^ (value >>> 14)) >>> 0) / 4_294_967_296;
    };
};

const alphabet = "abcdefghijklmnopqrstuvwxyz";
const ownerNames = ["Alice", "Bob", "Cara", "Dee", "Eli"] as const;
const logTypes = ["WARN", "INFO", "ERROR"] as const;
const tags = ["red", "green", "blue", "amber"] as const;

const randomString = (len: number, randInt: (max: number) => number) => {
    let out = "";
    for (let i = 0; i < len; i++) {
        out += alphabet[randInt(alphabet.length)]!;
    }
    return out;
};

const createRng = (seed: number) => {
    const rng = mulberry32(seed);
    const randInt = (max: number) => Math.floor(rng() * max);
    return { next: rng, randInt };
};

const generateData = (count: number, seed = 1337): Array<LargeItem> => {
    const { next, randInt } = createRng(seed);
    const data: Array<LargeItem> = [];
    for (let i = 0; i < count; i++) {
        const baseName = i % 7 === 0 ? `ab${randomString(4, randInt)}` : randomString(6, randInt);
        const name = i % 31 === 0 ? null : baseName;
        const score = i % 97 === 0 ? Number.NaN : randInt(50);
        const created = i % 2 === 0
            ? new Date(2026, 0, (i % 28) + 1)
            : `2026-01-${String((i % 28) + 1).padStart(2, "0")}T00:00:00.000Z`;
        const owner = ownerNames[randInt(ownerNames.length)]!;
        const nick = next() > 0.8 ? `${owner[0]}${randInt(9)}` : null;
        const logCount = randInt(3);
        const Logs: Array<{ tags: Array<string>; type: string; when: Date | string }> = [];
        for (let j = 0; j < logCount; j++) {
            const type = logTypes[randInt(logTypes.length)]!;
            const tagCount = randInt(3);
            const logTags: Array<string> = [];
            for (let k = 0; k < tagCount; k++) {
                logTags.push(tags[randInt(tags.length)]!);
            }
            const when = j % 2 === 0
                ? new Date(2026, 0, (i % 28) + 1)
                : `2026-01-${String((i % 28) + 1).padStart(2, "0")}T00:00:00.000Z`;
            Logs.push({ tags: logTags, type, when });
        }
        const flagCount = randInt(3);
        const flags: Array<string> = [];
        for (let j = 0; j < flagCount; j++) {
            flags.push(tags[randInt(tags.length)]!);
        }

        data.push({
            active: i % 2 === 0,
            created,
            flags,
            id: i,
            Logs,
            meta: { owner: { name: owner, nickname: nick } },
            name,
            score,
        });
    }
    return data;
};

const getRandomNamesArray = (): Array<string> => {
    const upper = randomInt(1, ownerNames.length + 1);
    const out: Array<string> = [];
    for (let i = 0; i < upper; i++) {
        const random = randomInt(0, ownerNames.length)
        out.push(ownerNames[random]!);
    }
    return out;
};

export {
    generateData,
    getRandomNamesArray,
    ownerNames,
    logTypes,
    tags,
    type LargeItem,
};
