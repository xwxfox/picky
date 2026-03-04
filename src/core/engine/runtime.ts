import type { IngressEngine } from "@/io/ingress";
import { QueryBuilder } from "./builder";

export class Engine {
    static from<T extends Record<string, unknown>>(ingress: IngressEngine<T>): QueryBuilder<T> {
        return QueryBuilder.from(ingress);
    }
}
