import { Engine, httpSource, IngressEngine, Schema } from "../src";

type ApiItem = {
    id: number;
    name: string;
    score: number;
};

const source = httpSource<ApiItem>({
    behavior: {
        baseUrl: "https://api.paws.com",
        dataPath: "data",
        method: "GET",
        pagination: { limitParam: "limit", mode: "offset", offsetParam: "offset", pageSize: 100 },
        path: "/items",
    },
    schema: Schema.inline<ApiItem>(),
});

const ingress = IngressEngine.fromSource(source);

const result = await Engine.from(ingress)
    .greaterThan("score", 10)
    .out()
    .orderBy("score", { direction: "desc" })
    .limit(20)
    .result();

console.dir(result, { depth: null });
