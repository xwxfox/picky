import { Schema } from "@/io/schema";
import type { ApiBehavior } from "@/io/ingress/adapters/http";

type OpenApiSchemaObject = {
    type?: string;
    properties?: Record<string, OpenApiSchemaObject>;
    items?: OpenApiSchemaObject;
    required?: Array<string>;
    allOf?: Array<OpenApiSchemaObject>;
    oneOf?: Array<OpenApiSchemaObject>;
    anyOf?: Array<OpenApiSchemaObject>;
    $ref?: string;
};

type OpenApiMediaType = {
    schema?: OpenApiSchemaObject;
};

type OpenApiResponse = {
    content?: Record<string, OpenApiMediaType>;
};

type OpenApiOperation = {
    responses?: Record<string, OpenApiResponse>;
};

export type OpenApiDocument = {
    openapi: string;
    paths: Record<string, Record<string, OpenApiOperation>>;
    components?: {
        schemas?: Record<string, OpenApiSchemaObject>;
    };
};

export type OpenApiExtractOptions = {
    dataPath?: string;
    method?: "get" | "post" | "put" | "patch" | "delete";
    responseStatus?: string;
    contentType?: string;
};

export type OpenApiBehavior<T extends Record<string, unknown>> = {
    behavior: ApiBehavior<T>;
    schema: Schema<T>;
};

export function openApiBehavior<T extends Record<string, unknown>>(
    doc: OpenApiDocument,
    path: string,
    options?: OpenApiExtractOptions
): OpenApiBehavior<T> {
    if (doc.openapi.startsWith("3.1")) {
        warnOnce("OpenAPI 3.1 detected. Parsed as 3.0 subset.");
    }
    const method = options?.method ?? "get";
    const pathItem = doc.paths[path];
    if (!pathItem) {throw new Error(`OpenAPI path not found: ${path}`);}
    const op = pathItem[method];
    if (!op) {throw new Error(`OpenAPI method not found: ${method.toUpperCase()} ${path}`);}
    const status = options?.responseStatus ?? "200";
    const response = op.responses?.[status] ?? op.responses?.["default"];
    if (!response) {throw new Error(`OpenAPI response not found for status: ${status}`);}
    const contentType = options?.contentType ?? "application/json";
    const media = response.content?.[contentType];
    if (!media?.schema) {throw new Error(`OpenAPI response schema not found for content type: ${contentType}`);}
    const schema = resolveSchema(doc, media.schema);
    const sample = schemaToSample(schema, doc);
    const typedSchema = Schema.infer<T>(sample as T);

    const behavior: ApiBehavior<T> = {
        baseUrl: "",
        dataPath: options?.dataPath,
        method: method.toUpperCase() as "GET" | "POST",
        path,
    };
    return { behavior, schema: typedSchema };
}

const warnCache = new Set<string>();

function warnOnce(message: string): void {
    if (warnCache.has(message)) {return;}
    warnCache.add(message);
    // eslint-disable-next-line no-console
    console.warn(message);
}

function resolveSchema(doc: OpenApiDocument, schema: OpenApiSchemaObject): OpenApiSchemaObject {
    if (schema.$ref) {
        const ref = schema.$ref;
        if (!ref.startsWith("#/")) {throw new Error(`Unsupported $ref: ${ref}`);}
        const parts = ref.slice(2).split("/");
        let current: unknown = doc as unknown;
        for (let i = 0; i < parts.length; i++) {
            if (!current || typeof current !== "object") {throw new Error(`Invalid $ref: ${ref}`);}
            current = (current as Record<string, unknown>)[parts[i]!];
        }
        if (!current || typeof current !== "object") {throw new Error(`Invalid $ref: ${ref}`);}
        return current as OpenApiSchemaObject;
    }
    if (schema.allOf && schema.allOf.length > 0) {
        return mergeAllOf(doc, schema.allOf);
    }
    if (schema.oneOf && schema.oneOf.length > 0) {
        return resolveSchema(doc, schema.oneOf[0]!);
    }
    if (schema.anyOf && schema.anyOf.length > 0) {
        return resolveSchema(doc, schema.anyOf[0]!);
    }
    return schema;
}

function mergeAllOf(doc: OpenApiDocument, schemas: Array<OpenApiSchemaObject>): OpenApiSchemaObject {
    const merged: OpenApiSchemaObject = { properties: {}, required: [], type: "object" };
    for (let i = 0; i < schemas.length; i++) {
        const resolved = resolveSchema(doc, schemas[i]!);
        if (resolved.type && resolved.type !== "object") {continue;}
        const props = resolved.properties ?? {};
        for (const key of Object.keys(props)) {
            (merged.properties as Record<string, OpenApiSchemaObject>)[key] = props[key]!;
        }
        const req = resolved.required ?? [];
        for (let j = 0; j < req.length; j++) {merged.required!.push(req[j]!);}
    }
    return merged;
}

function schemaToSample(schema: OpenApiSchemaObject, doc: OpenApiDocument): Record<string, unknown> {
    const resolved = resolveSchema(doc, schema);
    if (resolved.type === "object" || resolved.properties) {
        const output: Record<string, unknown> = {};
        const props = resolved.properties ?? {};
        for (const key of Object.keys(props)) {
            output[key] = schemaNodeToValue(props[key]!, doc);
        }
        return output;
    }
    return { value: schemaNodeToValue(resolved, doc) };
}

function schemaNodeToValue(schema: OpenApiSchemaObject, doc: OpenApiDocument): unknown {
    const resolved = resolveSchema(doc, schema);
    if (resolved.type === "string") {return "";}
    if (resolved.type === "integer" || resolved.type === "number") {return 0;}
    if (resolved.type === "boolean") {return false;}
    if (resolved.type === "array") {
        const item = resolved.items ? schemaNodeToValue(resolved.items, doc) : null;
        return item === null ? [] : [item];
    }
    if (resolved.type === "object" || resolved.properties) {
        const output: Record<string, unknown> = {};
        const props = resolved.properties ?? {};
        for (const key of Object.keys(props)) {
            output[key] = schemaNodeToValue(props[key]!, doc);
        }
        return output;
    }
    return null;
}
