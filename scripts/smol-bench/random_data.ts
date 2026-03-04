const { randomInt } = await import("node:crypto");

type LargeItem = {
    active: boolean;
    created: Date | string;
    flags: string[];
    id: number;
    Logs: Array<{ tags: string[]; type: string; when: Date | string }>;
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
    return { randInt, next: rng };
};

const generateData = (count: number, seed = 1337): LargeItem[] => {
    const { randInt, next } = createRng(seed);
    const data: LargeItem[] = [];
    for (let i = 0; i < count; i++) {
        const baseName = i % 7 === 0 ? `ab${randomString(4, randInt)}` : randomString(6, randInt);
        const name = i % 31 === 0 ? null : baseName;
        const score = i % 97 === 0 ? Number.NaN : randInt(50);
        const created = i % 2 === 0
            ? new Date(2024, 0, (i % 28) + 1)
            : `2024-01-${String((i % 28) + 1).padStart(2, "0")}T00:00:00.000Z`;
        const owner = ownerNames[randInt(ownerNames.length)]!;
        const nick = next() > 0.8 ? `${owner[0]}${randInt(9)}` : null;
        const logCount = randInt(3);
        const Logs: Array<{ tags: string[]; type: string; when: Date | string }> = [];
        for (let j = 0; j < logCount; j++) {
            const type = logTypes[randInt(logTypes.length)]!;
            const tagCount = randInt(3);
            const logTags: string[] = [];
            for (let k = 0; k < tagCount; k++) {
                logTags.push(tags[randInt(tags.length)]!);
            }
            const when = j % 2 === 0
                ? new Date(2024, 0, (i % 28) + 1)
                : `2024-01-${String((i % 28) + 1).padStart(2, "0")}T00:00:00.000Z`;
            Logs.push({ tags: logTags, type, when });
        }
        const flagCount = randInt(3);
        const flags: string[] = [];
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

const getRandomNamesArray = (): string[] => {
    const out: string[] = [];
    for (let i = 0; i < randomInt(1, ownerNames.length + 1); i++) {
        out.push(ownerNames[randomInt(ownerNames.length)]!);
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
