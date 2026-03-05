import type { Predicate } from "@/types";
import type { Schema } from "@/io/schema";
import type { DefaultSearchCapabilityState } from "@/types/search";
import { IngressEngine } from "@/io/ingress";
import { QueryBuilder } from "@/core/engine/builder";
import type { EgressEngine } from "@/io/egress";

export type QueryChainPlan<T extends Record<string, unknown>> = {
    hash: string;
    id: string;
    predicates: Array<Predicate<T>>;
    schema?: Schema<T>;
};

type CompiledCache<T extends Record<string, unknown>> = {
    hash: string;
    plan: QueryChainPlan<T>;
};

export class QueryChain<T extends Record<string, unknown>> {
    private static compiled = new Map<string, CompiledCache<Record<string, unknown>>>();
    private constructor(private readonly plan: QueryChainPlan<T>) {}

    static from<T extends Record<string, unknown>>(
        schema: Schema<T>,
        builder?: (q: QueryBuilder<T, DefaultSearchCapabilityState, "sync">) => QueryBuilder<T, DefaultSearchCapabilityState, "sync">
    ): QueryChain<T> {
        const ingress = IngressEngine.fromSchema(schema);
        const qb = builder ? builder(QueryBuilder.from(ingress)) : QueryBuilder.from(ingress);
        const plan = qb.compilePlan();
        const hash = plan.id;
        const cached = QueryChain.compiled.get(hash);
        if (cached) {
            return new QueryChain<T>(cached.plan as QueryChainPlan<T>);
        }
        const id = `chain_${hash}`;
        const chainPlan: QueryChainPlan<T> = {
            hash,
            id,
            predicates: plan.predicates,
            schema,
        };
        QueryChain.compiled.set(hash, { hash, plan: chainPlan } as CompiledCache<Record<string, unknown>>);
        return new QueryChain<T>(chainPlan);
    }

    builder(ingress: IngressEngine<T>): QueryBuilder<T, DefaultSearchCapabilityState, "sync"> {
        return QueryBuilder.from(ingress).use(this);
    }

    out(ingress: IngressEngine<T>): EgressEngine<T> {
        return QueryBuilder.from(ingress).use(this).out();
    }

    execute(ingress: IngressEngine<T>): EgressEngine<T> {
        return this.out(ingress);
    }

    getPlan(): QueryChainPlan<T> {
        return this.plan;
    }
}

export type ChainConfig<T extends Record<string, unknown>> = {
    name: string;
    predicate: Predicate<T>;
}

export class ChainRegistry<T extends Record<string, unknown>> {
    private chains = new Map<string, Predicate<T>>();

    register(name: string, predicate: Predicate<T>): void {
        this.chains.set(name, predicate);
    }

    get(name: string): Predicate<T> | undefined {
        return this.chains.get(name);
    }

    has(name: string): boolean {
        return this.chains.has(name);
    }

    remove(name: string): boolean {
        return this.chains.delete(name);
    }

    clear(): void {
        this.chains.clear();
    }

    names(): Array<string> {
        return [...this.chains.keys()];
    }
}

export class ReusableChain<T extends Record<string, unknown>> {
    private predicate: Predicate<T>;

    constructor(predicate: Predicate<T>) {
        this.predicate = predicate;
    }

    getPredicate(): Predicate<T> {
        return this.predicate;
    }

    and(other: ReusableChain<T>): ReusableChain<T> {
        const left = this.predicate;
        const right = other.predicate;
        return new ReusableChain<T>((item) => left(item) && right(item));
    }

    or(other: ReusableChain<T>): ReusableChain<T> {
        const left = this.predicate;
        const right = other.predicate;
        return new ReusableChain<T>((item) => left(item) || right(item));
    }

    not(): ReusableChain<T> {
        const original = this.predicate;
        return new ReusableChain<T>((item) => !original(item));
    }
}

export function createChain<T extends Record<string, unknown>>(
    predicate: Predicate<T>
): ReusableChain<T> {
    return new ReusableChain<T>(predicate);
}

export function compileChain<T extends Record<string, unknown>>(
    chain: ReusableChain<T>
): Predicate<T> {
    return chain.getPredicate();
}
