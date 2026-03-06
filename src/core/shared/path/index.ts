import type { ResolvePredicate, ResolveObject, ResolveValue, ResolveSortKey, SortKey } from "@/types";

export type PathAccessors = {
    segments: Array<string>;
    some: (obj: ResolveObject, predicate: ResolvePredicate) => boolean;
    every: (obj: ResolveObject, predicate: ResolvePredicate) => boolean;
    forEach: (obj: ResolveObject, visit: (value: ResolveValue) => void) => void;
    first: (obj: ResolveObject) => ResolveValue | undefined;
    exists: (obj: ResolveObject) => boolean;
};

const accessorsBySegments = new WeakMap<Array<string>, PathAccessors>();

function getAccessorsForSegments(segments: Array<string>): PathAccessors {
    const cached = accessorsBySegments.get(segments);
    if (cached) {return cached;}
    const created = createPathAccessors(segments);
    accessorsBySegments.set(segments, created);
    return created;
}

export function createPathAccessors(segments: Array<string>): PathAccessors {
    const length = segments.length;
    if (length === 1) {
        const segment0 = segments[0]!;
        return {
            every: (obj, predicate) => {
                if (obj == null || typeof obj !== "object") {return false;}
                const resolved = (obj)[segment0];
                if (Array.isArray(resolved)) {
                    if (resolved.length === 0) {return false;}
                    for (let i = 0; i < resolved.length; i++) {
                        if (!predicate(resolved[i])) {return false;}
                    }
                    return true;
                }
                return predicate(resolved as ResolveValue);
            },
            exists: (obj) => {
                if (obj == null || typeof obj !== "object") {return false;}
                return Object.hasOwn(obj, segment0);
            },
            first: (obj) => {
                if (obj == null || typeof obj !== "object") {return undefined;}
                const resolved = (obj)[segment0];
                if (Array.isArray(resolved)) {return resolved.length > 0 ? (resolved[0]) : undefined;}
                return resolved as ResolveValue;
            },
            forEach: (obj, visit) => {
                if (obj == null || typeof obj !== "object") {return;}
                const resolved = (obj)[segment0];
                if (Array.isArray(resolved)) {
                    for (let i = 0; i < resolved.length; i++) {
                        visit(resolved[i]);
                    }
                    return;
                }
                visit(resolved as ResolveValue);
            },
            segments,
            some: (obj, predicate) => {
                if (obj == null || typeof obj !== "object") {return false;}
                const resolved = (obj)[segment0];
                if (Array.isArray(resolved)) {
                    for (let i = 0; i < resolved.length; i++) {
                        if (predicate(resolved[i])) {return true;}
                    }
                    return false;
                }
                return predicate(resolved as ResolveValue);
            },
        };
    }

    if (length === 2) {
        const first = segments[0]!;
        const second = segments[1]!;
        return {
            every: (obj, predicate) => {
                if (obj == null || typeof obj !== "object") {return false;}
                const resolved = (obj)[first];
                let found = false;
                if (Array.isArray(resolved)) {
                    for (let i = 0; i < resolved.length; i++) {
                        const entry = resolved[i];
                        if (entry == null || typeof entry !== "object") {continue;}
                        const value = (entry as ResolveObject)[second];
                        if (Array.isArray(value)) {continue;}
                        found = true;
                        if (!predicate(value as ResolveValue)) {return false;}
                    }
                    return found;
                }
                if (resolved == null || typeof resolved !== "object") {return false;}
                const value = (resolved as ResolveObject)[second];
                if (Array.isArray(value)) {
                    if (value.length === 0) {return false;}
                    for (let i = 0; i < value.length; i++) {
                        found = true;
                        if (!predicate(value[i])) {return false;}
                    }
                    return found;
                }
                return predicate(value as ResolveValue);
            },
            exists: (obj) => {
                if (obj == null || typeof obj !== "object") {return false;}
                if (!Object.hasOwn(obj, first)) {return false;}
                const resolved = (obj as ResolveObject)[first];
                if (resolved == null) {return false;}
                if (typeof resolved !== "object") {return false;}
                if (Array.isArray(resolved)) {
                    for (let i = 0; i < resolved.length; i++) {
                        const entry = resolved[i];
                        if (entry == null || typeof entry !== "object") {continue;}
                        const entryObj = entry as ResolveObject;
                        if (!Object.hasOwn(entryObj, second)) {continue;}
                        return true;
                    }
                    return false;
                }
                return Object.hasOwn(resolved as ResolveObject, second);
            },
            first: (obj) => {
                if (obj == null || typeof obj !== "object") {return undefined;}
                const resolved = (obj)[first];
                if (Array.isArray(resolved)) {
                    for (let i = 0; i < resolved.length; i++) {
                        const entry = resolved[i];
                        if (entry == null || typeof entry !== "object") {continue;}
                        const value = (entry as ResolveObject)[second];
                        if (Array.isArray(value)) {continue;}
                        return value as ResolveValue;
                    }
                    return undefined;
                }
                if (resolved == null || typeof resolved !== "object") {return undefined;}
                const value = (resolved as ResolveObject)[second];
                if (Array.isArray(value)) {
                    return value.length > 0 ? (value[0]) : undefined;
                }
                return value as ResolveValue;
            },
            forEach: (obj, visit) => {
                if (obj == null || typeof obj !== "object") {return;}
                const resolved = (obj)[first];
                if (Array.isArray(resolved)) {
                    for (let i = 0; i < resolved.length; i++) {
                        const entry = resolved[i];
                        if (entry == null || typeof entry !== "object") {continue;}
                        const value = (entry as ResolveObject)[second];
                        if (Array.isArray(value)) {continue;}
                        visit(value as ResolveValue);
                    }
                    return;
                }
                if (resolved == null || typeof resolved !== "object") {return;}
                const value = (resolved as ResolveObject)[second];
                if (Array.isArray(value)) {
                    for (let i = 0; i < value.length; i++) {
                        visit(value[i]);
                    }
                    return;
                }
                visit(value as ResolveValue);
            },
            segments,
            some: (obj, predicate) => {
                if (obj == null || typeof obj !== "object") {return false;}
                const resolved = (obj)[first];
                if (Array.isArray(resolved)) {
                    for (let i = 0; i < resolved.length; i++) {
                        const entry = resolved[i];
                        if (entry == null || typeof entry !== "object") {continue;}
                        const value = (entry as ResolveObject)[second];
                        if (Array.isArray(value)) {continue;}
                        if (predicate(value as ResolveValue)) {
                            return true;
                        }
                    }
                    return false;
                }
                if (resolved == null || typeof resolved !== "object") {return false;}
                const value = (resolved as ResolveObject)[second];
                if (Array.isArray(value)) {
                    for (let i = 0; i < value.length; i++) {
                        if (predicate(value[i])) {return true;}
                    }
                    return false;
                }
                return predicate(value as ResolveValue);
            },
        };
    }

    if (length === 3) {
        const first = segments[0]!;
        const second = segments[1]!;
        const third = segments[2]!;
        return {
            every: (obj, predicate) => {
                if (obj == null || typeof obj !== "object") {return false;}
                const resolved = (obj)[first];
                let found = false;
                if (Array.isArray(resolved)) {
                    for (let i = 0; i < resolved.length; i++) {
                        const entry = resolved[i];
                        if (entry == null || typeof entry !== "object") {continue;}
                        const value = (entry as ResolveObject)[second];
                        if (Array.isArray(value)) {continue;}
                        if (value == null || typeof value !== "object") {continue;}
                        const leaf = (value as ResolveObject)[third];
                        if (Array.isArray(leaf)) {continue;}
                        found = true;
                        if (!predicate(leaf as ResolveValue)) {return false;}
                    }
                    return found;
                }
                if (resolved == null || typeof resolved !== "object") {return false;}
                const value = (resolved as ResolveObject)[second];
                if (Array.isArray(value)) {
                    for (let i = 0; i < value.length; i++) {
                        const entry = value[i];
                        if (entry == null || typeof entry !== "object") {continue;}
                        const leaf = (entry as ResolveObject)[third];
                        if (Array.isArray(leaf)) {continue;}
                        found = true;
                        if (!predicate(leaf as ResolveValue)) {return false;}
                    }
                    return found;
                }
                if (value == null || typeof value !== "object") {return false;}
                const leaf = (value as ResolveObject)[third];
                if (Array.isArray(leaf)) {
                    if (leaf.length === 0) {return false;}
                    for (let i = 0; i < leaf.length; i++) {
                        if (!predicate(leaf[i])) {return false;}
                    }
                    return true;
                }
                return predicate(leaf as ResolveValue);
            },
            exists: (obj) => {
                if (obj == null || typeof obj !== "object") {return false;}
                if (!Object.hasOwn(obj, first)) {return false;}
                const resolved = (obj as ResolveObject)[first];
                if (resolved == null || typeof resolved !== "object") {return false;}
                if (Array.isArray(resolved)) {
                    for (let i = 0; i < resolved.length; i++) {
                        const entry = resolved[i];
                        if (entry == null || typeof entry !== "object") {continue;}
                        if (!Object.hasOwn(entry as ResolveObject, second)) {continue;}
                        const value = (entry as ResolveObject)[second];
                        if (value == null || typeof value !== "object") {continue;}
                        if (Array.isArray(value)) {continue;}
                        if (Object.hasOwn(value as ResolveObject, third)) {return true;}
                    }
                    return false;
                }
                if (!Object.hasOwn(resolved as ResolveObject, second)) {return false;}
                const value = (resolved as ResolveObject)[second];
                if (value == null || typeof value !== "object") {return false;}
                if (Array.isArray(value)) {
                    for (let i = 0; i < value.length; i++) {
                        const leaf = value[i];
                        if (leaf == null || typeof leaf !== "object") {continue;}
                        if (Object.hasOwn(leaf as ResolveObject, third)) {return true;}
                    }
                    return false;
                }
                return Object.hasOwn(value as ResolveObject, third);
            },
            first: (obj) => {
                if (obj == null || typeof obj !== "object") {return undefined;}
                const resolved = (obj)[first];
                if (Array.isArray(resolved)) {
                    for (let i = 0; i < resolved.length; i++) {
                        const entry = resolved[i];
                        if (entry == null || typeof entry !== "object") {continue;}
                        const value = (entry as ResolveObject)[second];
                        if (Array.isArray(value)) {continue;}
                        if (value == null || typeof value !== "object") {continue;}
                        const leaf = (value as ResolveObject)[third];
                        if (Array.isArray(leaf)) {continue;}
                        return leaf as ResolveValue;
                    }
                    return undefined;
                }
                if (resolved == null || typeof resolved !== "object") {return undefined;}
                const value = (resolved as ResolveObject)[second];
                if (Array.isArray(value)) {
                    for (let i = 0; i < value.length; i++) {
                        const entry = value[i];
                        if (entry == null || typeof entry !== "object") {continue;}
                        const leaf = (entry as ResolveObject)[third];
                        if (Array.isArray(leaf)) {continue;}
                        return leaf as ResolveValue;
                    }
                    return undefined;
                }
                if (value == null || typeof value !== "object") {return undefined;}
                const leaf = (value as ResolveObject)[third];
                if (Array.isArray(leaf)) {return leaf.length > 0 ? (leaf[0]) : undefined;}
                return leaf as ResolveValue;
            },
            forEach: (obj, visit) => {
                if (obj == null || typeof obj !== "object") {return;}
                const resolved = (obj)[first];
                if (Array.isArray(resolved)) {
                    for (let i = 0; i < resolved.length; i++) {
                        const entry = resolved[i];
                        if (entry == null || typeof entry !== "object") {continue;}
                        const value = (entry as ResolveObject)[second];
                        if (Array.isArray(value)) {continue;}
                        if (value == null || typeof value !== "object") {continue;}
                        const leaf = (value as ResolveObject)[third];
                        if (Array.isArray(leaf)) {continue;}
                        visit(leaf as ResolveValue);
                    }
                    return;
                }
                if (resolved == null || typeof resolved !== "object") {return;}
                const value = (resolved as ResolveObject)[second];
                if (Array.isArray(value)) {
                    for (let i = 0; i < value.length; i++) {
                        const entry = value[i];
                        if (entry == null || typeof entry !== "object") {continue;}
                        const leaf = (entry as ResolveObject)[third];
                        if (Array.isArray(leaf)) {continue;}
                        visit(leaf as ResolveValue);
                    }
                    return;
                }
                if (value == null || typeof value !== "object") {return;}
                const leaf = (value as ResolveObject)[third];
                if (Array.isArray(leaf)) {
                    for (let i = 0; i < leaf.length; i++) {
                        visit(leaf[i]);
                    }
                    return;
                }
                visit(leaf as ResolveValue);
            },
            segments,
            some: (obj, predicate) => {
                if (obj == null || typeof obj !== "object") {return false;}
                const resolved = (obj)[first];
                if (Array.isArray(resolved)) {
                    for (let i = 0; i < resolved.length; i++) {
                        const entry = resolved[i];
                        if (entry == null || typeof entry !== "object") {continue;}
                        const value = (entry as ResolveObject)[second];
                        if (Array.isArray(value)) {continue;}
                        if (value == null || typeof value !== "object") {continue;}
                        const leaf = (value as ResolveObject)[third];
                        if (Array.isArray(leaf)) {continue;}
                        if (predicate(leaf as ResolveValue)) {
                            return true;
                        }
                    }
                    return false;
                }
                if (resolved == null || typeof resolved !== "object") {return false;}
                const value = (resolved as ResolveObject)[second];
                if (Array.isArray(value)) {
                    for (let i = 0; i < value.length; i++) {
                        const entry = value[i];
                        if (entry == null || typeof entry !== "object") {continue;}
                        const leaf = (entry as ResolveObject)[third];
                        if (Array.isArray(leaf)) {continue;}
                        if (predicate(leaf as ResolveValue)) {
                            return true;
                        }
                    }
                    return false;
                }
                if (value == null || typeof value !== "object") {return false;}
                const leaf = (value as ResolveObject)[third];
                if (Array.isArray(leaf)) {
                    for (let i = 0; i < leaf.length; i++) {
                        if (predicate(leaf[i])) {return true;}
                    }
                    return false;
                }
                return predicate(leaf as ResolveValue);
            },
        };
    }

    return {
        every: (obj, predicate) => {
            if (obj == null || typeof obj !== "object") {return false;}
            let current: Array<ResolveValue> = [obj as ResolveValue];
            let next: Array<ResolveValue> = [];
            let seenArray = false;
            let found = false;

            for (let i = 0; i < segments.length; i++) {
                const segment = segments[i]!;
                const isLast = i === segments.length - 1;
                let nextIndex = 0;

                for (let j = 0; j < current.length; j++) {
                    const value = current[j];
                    if (value == null) {continue;}
                    if (typeof value !== "object") {continue;}

                    const resolved = (value as ResolveObject)[segment];

                    if (Array.isArray(resolved)) {
                        if (seenArray) {continue;}
                        if (!isLast) {seenArray = true;}
                        if (isLast) {
                            if (resolved.length === 0) {continue;}
                            for (let k = 0; k < resolved.length; k++) {
                                found = true;
                                if (!predicate(resolved[k])) {return false;}
                            }
                        } else {
                            for (let k = 0; k < resolved.length; k++) {
                                next[nextIndex++] = resolved[k];
                            }
                        }
                    } else if (isLast) {
                        found = true;
                        if (!predicate(resolved as ResolveValue)) {return false;}
                    } else {
                        next[nextIndex++] = resolved as ResolveValue;
                    }
                }

                if (isLast) {return found;}

                next.length = nextIndex;
                const temp = current;
                current = next;
                next = temp;
            }

            return found;
        },
        exists: (obj) => {
            if (segments.length === 0) {return false;}
            if (obj == null || typeof obj !== "object") {return false;}
            let current: Array<ResolveValue> = [obj as ResolveValue];
            let next: Array<ResolveValue> = [];
            let seenArray = false;

            for (let i = 0; i < segments.length; i++) {
                const segment = segments[i]!;
                const isLast = i === segments.length - 1;
                let nextIndex = 0;

                for (let j = 0; j < current.length; j++) {
                    const value = current[j];
                    if (value == null) {continue;}
                    if (typeof value !== "object") {continue;}

                    const objValue = value as ResolveObject;
                    if (!Object.hasOwn(objValue, segment)) {continue;}
                    const resolved = objValue[segment];
                    if (isLast) {return true;}

                    if (Array.isArray(resolved)) {
                        if (seenArray) {continue;}
                        seenArray = true;
                        for (let k = 0; k < resolved.length; k++) {
                            next[nextIndex++] = resolved[k];
                        }
                    } else {
                        next[nextIndex++] = resolved as ResolveValue;
                    }
                }

                next.length = nextIndex;
                const temp = current;
                current = next;
                next = temp;
            }

            return false;
        },
        first: (obj) => {
            if (obj == null || typeof obj !== "object") {return undefined;}
            let current: Array<ResolveValue> = [obj as ResolveValue];
            let next: Array<ResolveValue> = [];
            let seenArray = false;

            for (let i = 0; i < segments.length; i++) {
                const segment = segments[i]!;
                const isLast = i === segments.length - 1;
                let nextIndex = 0;

                for (let j = 0; j < current.length; j++) {
                    const value = current[j];
                    if (value == null) {continue;}
                    if (typeof value !== "object") {continue;}

                    const resolved = (value as ResolveObject)[segment];

                    if (Array.isArray(resolved)) {
                        if (seenArray) {continue;}
                        if (isLast) {
                            return resolved.length > 0 ? (resolved[0]) : undefined;
                        }
                        seenArray = true;
                        for (let k = 0; k < resolved.length; k++) {
                            next[nextIndex++] = resolved[k];
                        }
                    } else if (isLast) {
                        return resolved as ResolveValue;
                    } else {
                        next[nextIndex++] = resolved as ResolveValue;
                    }
                }

                if (isLast) {return undefined;}

                next.length = nextIndex;
                const temp = current;
                current = next;
                next = temp;
            }

            return undefined;
        },
        forEach: (obj, visit) => {
            if (obj == null || typeof obj !== "object") {return;}
            let current: Array<ResolveValue> = [obj as ResolveValue];
            let next: Array<ResolveValue> = [];
            let seenArray = false;

            for (let i = 0; i < segments.length; i++) {
                const segment = segments[i]!;
                const isLast = i === segments.length - 1;
                let nextIndex = 0;

                for (let j = 0; j < current.length; j++) {
                    const value = current[j];
                    if (value == null) {continue;}
                    if (typeof value !== "object") {continue;}

                    const resolved = (value as ResolveObject)[segment];

                    if (Array.isArray(resolved)) {
                        if (seenArray) {continue;}
                        if (isLast) {
                            for (let k = 0; k < resolved.length; k++) {
                                visit(resolved[k]);
                            }
                        } else {
                            seenArray = true;
                            for (let k = 0; k < resolved.length; k++) {
                                next[nextIndex++] = resolved[k];
                            }
                        }
                    } else if (isLast) {
                        visit(resolved as ResolveValue);
                    } else {
                        next[nextIndex++] = resolved as ResolveValue;
                    }
                }

                if (isLast) {return;}

                next.length = nextIndex;
                const temp = current;
                current = next;
                next = temp;
            }
        },
        segments,
        some: (obj, predicate) => {
            if (obj == null || typeof obj !== "object") {return false;}
            let current: Array<ResolveValue> = [obj as ResolveValue];
            let next: Array<ResolveValue> = [];
            let seenArray = false;

            for (let i = 0; i < segments.length; i++) {
                const segment = segments[i]!;
                const isLast = i === segments.length - 1;
                let nextIndex = 0;

                for (let j = 0; j < current.length; j++) {
                    const value = current[j];
                    if (value == null) {continue;}
                    if (typeof value !== "object") {continue;}

                    const resolved = (value as ResolveObject)[segment];

                    if (Array.isArray(resolved)) {
                        if (seenArray) {continue;}
                        if (!isLast) {seenArray = true;}
                        for (let k = 0; k < resolved.length; k++) {
                            const entry = resolved[k];
                            if (isLast) {
                                if (predicate(entry)) {return true;}
                            } else {
                                next[nextIndex++] = entry;
                            }
                        }
                    } else if (isLast) {
                        if (predicate(resolved as ResolveValue)) {return true;}
                    } else {
                        next[nextIndex++] = resolved as ResolveValue;
                    }
                }

                if (isLast) {return false;}

                next.length = nextIndex;
                const temp = current;
                current = next;
                next = temp;
            }

            return false;
        },
    };
}

export function someResolvedWithSegments(
    obj: ResolveObject,
    segments: Array<string>,
    predicate: ResolvePredicate
): boolean {
    return getAccessorsForSegments(segments).some(obj, predicate);
}

export function resolveFirstWithSegments(
    obj: ResolveObject,
    segments: Array<string>
): ResolveValue | undefined {
    return getAccessorsForSegments(segments).first(obj);
}

export function forEachResolvedWithSegments(
    obj: ResolveObject,
    segments: Array<string>,
    visit: (value: ResolveValue) => void
): void {
    getAccessorsForSegments(segments).forEach(obj, visit);
}

export function everyResolvedWithSegments(
    obj: ResolveObject,
    segments: Array<string>,
    predicate: ResolvePredicate
): boolean {
    return getAccessorsForSegments(segments).every(obj, predicate);
}

export function pathExistsWithSegments(obj: ResolveObject, segments: Array<string>): boolean {
    return getAccessorsForSegments(segments).exists(obj);
}

export function resolveOrderValueWithSegments(
    obj: ResolveObject,
    segments: Array<string>,
    resolve: ResolveSortKey
): SortKey | null {
    const value = resolveFirstWithSegments(obj, segments);
    if (value === undefined || value === null) {return null;}
    return resolve(value);
}
