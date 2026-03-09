import { cc, ptr } from "bun:ffi";
import { rmSync, writeFileSync } from "node:fs";
import type { PrefilterPlan } from "@/io/ingress/prefilter";

type PrefilterSymbol = (input: number, length: number) => number;
type PrefilterBatchSymbol = (input: number, inputLen: number, output: number, outputCapacity: number) => number;

export type PrefilterProgram = {
  fn: PrefilterSymbol;
  fnNdjson: PrefilterBatchSymbol;
  fnJsonArray: PrefilterBatchSymbol;
  key: string;
  cleanup: () => Promise<void>;
};

const programCache = new Map<string, PrefilterProgram>();
const cacheOrder: Array<string> = [];
const maxCacheSize = 64;
const encoder = new TextEncoder();

export function getPrefilterProgram(plan: PrefilterPlan): PrefilterProgram {
  const key = plan.key;
  const cached = programCache.get(key);
  if (cached) {
    return cached;
  }
  const source = buildPrefilterSource(plan);
  const filename = `prefilter-${key.replace(/[^a-zA-Z0-9_-]/g, "_")}-${Date.now()}.c`;
  const path = `/tmp/${filename}`;
  writeFileSync(path, source);
  const compiled = cc({
    source: path,
    flags: ["-O3"],
    symbols: {
      prefilter: {
        args: ["ptr", "usize"],
        returns: "i32",
      },
      prefilter_ndjson: {
        args: ["ptr", "usize", "ptr", "usize"],
        returns: "i32",
      },
      prefilter_json_array: {
        args: ["ptr", "usize", "ptr", "usize"],
        returns: "i32",
      },
    },
  });
  const program: PrefilterProgram = {
    fn: compiled.symbols.prefilter,
    fnNdjson: compiled.symbols.prefilter_ndjson,
    fnJsonArray: compiled.symbols.prefilter_json_array,
    key,
    cleanup: async () => {
      await compiled.close();
      try { rmSync(path); } catch { } // best effort cleanup of temp file
    }
  };
  programCache.set(key, program);
  cacheOrder.push(key);
  if (cacheOrder.length > maxCacheSize) {
    const evicted = cacheOrder.shift();
    if (evicted) {
      const evictedProgram = programCache.get(evicted);
      if (evictedProgram) {
        evictedProgram.cleanup();
      }
      programCache.delete(evicted);
    }
  }
  return program;
}

export function runPrefilter(program: PrefilterProgram, bytes: Uint8Array): number {
  const pointer = ptr(bytes);
  return program.fn(pointer, bytes.byteLength);
}

// Field tree node for nested path support.
// Each node can be a branch (has children) and/or a leaf (has predicateIndices).
type FieldNode = {
  children: Map<string, FieldNode>;
  predicateIndices: number[];
};

function buildFieldTree(plan: PrefilterPlan): FieldNode {
  const root: FieldNode = { children: new Map(), predicateIndices: [] };
  for (let i = 0; i < plan.predicates.length; i++) {
    const pred = plan.predicates[i]!;
    const segs = pred.segments ? pred.segments : [pred.field];
    let node = root;
    for (let s = 0; s < segs.length - 1; s++) {
      const seg = segs[s]!;
      let child = node.children.get(seg);
      if (!child) {
        child = { children: new Map(), predicateIndices: [] };
        node.children.set(seg, child);
      }
      node = child;
    }
    const lastSeg = segs[segs.length - 1]!;
    let leaf = node.children.get(lastSeg);
    if (!leaf) {
      leaf = { children: new Map(), predicateIndices: [] };
      node.children.set(lastSeg, leaf);
    }
    leaf.predicateIndices.push(i);
  }
  return root;
}

function buildPrefilterSource(plan: PrefilterPlan): string {
  const predicates = plan.predicates;
  const fieldTree = buildFieldTree(plan);

  const predicateState = predicates.map((_pred, index) => `int p${index}_seen = 0; int p${index}_pass = 0;`).join("\n  ");
  const predicateEval = predicates.map((pred, index) => {
    if (pred.op === "ne") {
      return `if (!p${index}_seen) { p${index}_pass = 1; } if (!p${index}_pass) { return 0; }`;
    }
    if (pred.op === "notIn") {
      return `if (!p${index}_seen) { p${index}_pass = 1; } if (!p${index}_pass) { return 0; }`;
    }
    return `if (!p${index}_seen) { p${index}_pass = 0; } if (!p${index}_pass) { return 0; }`;
  }).join("\n  ");

  const rootMatching = renderNodeMatching(fieldTree, plan, 0);

  return String.raw`
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <stdbool.h>
#include <stdlib.h>

// Fast path JSON object scanner with nested field support.
// Returns 1 = match, 0 = fail, -1 = unknown (parse required).
// - If we see something we don't understand (arrays/objects, bad escapes), we return -1.
// - That means "fall back to JSON.parse" in JS, so we never drop valid data.

static inline int is_space(char c) {
  return c == ' ' || c == '\n' || c == '\r' || c == '\t';
}

static const char* skip_ws(const char* p, const char* end) {
  while (p < end && is_space(*p)) { p++; }
  return p;
}

static int scan_string(const char** pptr, const char* end, const char** out, size_t* out_len, bool* needs_unescape) {
  const char* p = *pptr;
  if (p >= end || *p != '"') { return 0; }
  p++;
  const char* start = p;
  bool escape = false;
  while (p < end) {
    char c = *p;
    if (c == '\\') {
      escape = true;
      p++;
      if (p >= end) { return 0; }
      p++;
      continue;
    }
    if (c == '"') {
      *out = start;
      *out_len = (size_t)(p - start);
      *needs_unescape = escape;
      p++;
      *pptr = p;
      return 1;
    }
    p++;
  }
  return 0;
}

static int decode_hex(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return 10 + (c - 'a');
  if (c >= 'A' && c <= 'F') return 10 + (c - 'A');
  return -1;
}

// Minimal JSON string unescape ("\\", "\"", "\/", "\b", "\f", "\n", "\r", "\t", "\\uXXXX").
// We only run this when a string contains a backslash.
static int unescape_string(const char* input, size_t len, char* output, size_t* out_len) {
  size_t i = 0;
  size_t o = 0;
  while (i < len) {
    char c = input[i++];
    if (c != '\\') {
      output[o++] = c;
      continue;
    }
    if (i >= len) { return 0; }
    char esc = input[i++];
    switch (esc) {
      case '"': output[o++] = '"'; break;
      case '\\': output[o++] = '\\'; break;
      case '/': output[o++] = '/'; break;
      case 'b': output[o++] = '\b'; break;
      case 'f': output[o++] = '\f'; break;
      case 'n': output[o++] = '\n'; break;
      case 'r': output[o++] = '\r'; break;
      case 't': output[o++] = '\t'; break;
      case 'u': {
        if (i + 4 > len) { return 0; }
        int h1 = decode_hex(input[i]);
        int h2 = decode_hex(input[i + 1]);
        int h3 = decode_hex(input[i + 2]);
        int h4 = decode_hex(input[i + 3]);
        if (h1 < 0 || h2 < 0 || h3 < 0 || h4 < 0) { return 0; }
        int code = (h1 << 12) | (h2 << 8) | (h3 << 4) | h4;
        i += 4;
        if (code <= 0x7F) {
          output[o++] = (char)code;
        } else if (code <= 0x7FF) {
          output[o++] = (char)(0xC0 | ((code >> 6) & 0x1F));
          output[o++] = (char)(0x80 | (code & 0x3F));
        } else {
          output[o++] = (char)(0xE0 | ((code >> 12) & 0x0F));
          output[o++] = (char)(0x80 | ((code >> 6) & 0x3F));
          output[o++] = (char)(0x80 | (code & 0x3F));
        }
        break;
      }
      default:
        return 0;
    }
  }
  *out_len = o;
  return 1;
}

// Hand-rolled fast number parser. Parses integer part via multiply-add,
// only falls back to float arithmetic when '.' or 'e'/'E' is seen.
static int parse_number(const char** pptr, const char* end, double* out) {
  const char* p = *pptr;
  if (p >= end) { return 0; }

  int negative = 0;
  if (*p == '-') { negative = 1; p++; }
  if (p >= end || (*p < '0' || *p > '9')) { return 0; }

  // Integer part via multiply-add
  int64_t int_part = 0;
  if (*p == '0') {
    p++;
  } else {
    while (p < end && *p >= '0' && *p <= '9') {
      int_part = int_part * 10 + (*p - '0');
      p++;
    }
  }

  int has_frac = 0;
  double frac_part = 0.0;
  if (p < end && *p == '.') {
    has_frac = 1;
    p++;
    if (p >= end || *p < '0' || *p > '9') { return 0; }
    double frac_scale = 0.1;
    while (p < end && *p >= '0' && *p <= '9') {
      frac_part += (*p - '0') * frac_scale;
      frac_scale *= 0.1;
      p++;
    }
  }

  int has_exp = 0;
  int exp_val = 0;
  if (p < end && (*p == 'e' || *p == 'E')) {
    has_exp = 1;
    p++;
    int exp_neg = 0;
    if (p < end && *p == '+') { p++; }
    else if (p < end && *p == '-') { exp_neg = 1; p++; }
    if (p >= end || *p < '0' || *p > '9') { return 0; }
    while (p < end && *p >= '0' && *p <= '9') {
      exp_val = exp_val * 10 + (*p - '0');
      p++;
    }
    if (exp_neg) { exp_val = -exp_val; }
  }

  double result;
  if (!has_frac && !has_exp) {
    // Pure integer fast path
    result = (double)int_part;
  } else {
    result = (double)int_part + frac_part;
    if (has_exp) {
      // Apply exponent
      double base = 10.0;
      int abs_exp = exp_val < 0 ? -exp_val : exp_val;
      double factor = 1.0;
      while (abs_exp > 0) {
        if (abs_exp & 1) { factor *= base; }
        base *= base;
        abs_exp >>= 1;
      }
      if (exp_val < 0) {
        result /= factor;
      } else {
        result *= factor;
      }
    }
  }

  if (negative) { result = -result; }
  *out = result;
  *pptr = p;
  return 1;
}

static int skip_nested_value(const char** pptr, const char* end) {
  const char* p = *pptr;
  if (p >= end || (*p != '{' && *p != '[')) { return 0; }
  char stack[64];
  int top = 0;
  stack[top++] = (*p == '{') ? '}' : ']';
  p++;
  bool in_string = false;
  bool escape = false;
  while (p < end) {
    char c = *p;
    if (in_string) {
      if (escape) {
        escape = false;
      } else if (c == '\\') {
        escape = true;
      } else if (c == '"') {
        in_string = false;
      }
      p++;
      continue;
    }
    if (c == '"') {
      in_string = true;
      p++;
      continue;
    }
    if (c == '{' || c == '[') {
      if (top >= (int)(sizeof(stack) / sizeof(stack[0]))) { return 0; }
      stack[top++] = (c == '{') ? '}' : ']';
      p++;
      continue;
    }
    if (c == '}' || c == ']') {
      if (top == 0 || c != stack[top - 1]) { return 0; }
      top--;
      p++;
      if (top == 0) {
        *pptr = p;
        return 1;
      }
      continue;
    }
    p++;
  }
  return 0;
}

static int skip_value(const char** pptr, const char* end) {
  const char* p = *pptr;
  if (p >= end) { return 0; }
  if (*p == '"') {
    const char* value_start = NULL; size_t value_len = 0; bool needs_unescape = false;
    if (!scan_string(&p, end, &value_start, &value_len, &needs_unescape)) { return 0; }
    *pptr = p;
    return 1;
  }
  if (*p == '-' || (*p >= '0' && *p <= '9')) {
    double number = 0;
    if (!parse_number(&p, end, &number)) { return 0; }
    *pptr = p;
    return 1;
  }
  if (p + 4 <= end && memcmp(p, "true", 4) == 0) { p += 4; *pptr = p; return 1; }
  if (p + 5 <= end && memcmp(p, "false", 5) == 0) { p += 5; *pptr = p; return 1; }
  if (p + 4 <= end && memcmp(p, "null", 4) == 0) { p += 4; *pptr = p; return 1; }
  if (*p == '{' || *p == '[') {
    if (!skip_nested_value(&p, end)) { return 0; }
    *pptr = p;
    return 1;
  }
  return 0;
}

// Compare a JSON string literal to a target string.
// Uses byte-level compare when there are no escapes.
static int match_string(const char* input, size_t len, const char* target, size_t target_len, bool needs_unescape) {
  if (!needs_unescape) {
    if (len != target_len) { return 0; }
    return memcmp(input, target, len) == 0 ? 1 : 0;
  }
  // Use a stack buffer for small strings, heap for larger values.
  if (len < 256) {
    char buffer[256];
    size_t out_len = 0;
    if (!unescape_string(input, len, buffer, &out_len)) { return -1; }
    if (out_len != target_len) { return 0; }
    return memcmp(buffer, target, out_len) == 0 ? 1 : 0;
  }
  if (len > 8192) { return -1; }
  char* dyn = (char*)malloc(len + 1);
  if (!dyn) { return -1; }
  size_t out_len = 0;
  int ok = unescape_string(input, len, dyn, &out_len);
  if (!ok) { free(dyn); return -1; }
  int match = (out_len == target_len && memcmp(dyn, target, out_len) == 0) ? 1 : 0;
  free(dyn);
  return match;
}

// Per-object prefilter. Scans a single JSON object at [input, input+length).
// Returns 1 = match, 0 = fail, -1 = unknown (parse required).
static int prefilter_object(const char* input, size_t length) {
  const char* p = input;
  const char* end = input + length;
  p = skip_ws(p, end);
  if (p >= end || *p != '{') { return -1; }
  p++;

  ${predicateState}

  while (p < end) {
    p = skip_ws(p, end);
    if (p >= end) { return -1; }
    if (*p == '}') { p++; break; }

    const char* key_start = NULL;
    size_t key_len = 0;
    bool key_needs_unescape = false;
    if (!scan_string(&p, end, &key_start, &key_len, &key_needs_unescape)) { return -1; }
    // We don't currently support escaped keys.
    if (key_needs_unescape) { return -1; }
    p = skip_ws(p, end);
    if (p >= end || *p != ':') { return -1; }
    p++;
    p = skip_ws(p, end);

    if (p >= end) { return -1; }
    int handled = 0;

    ${rootMatching}

    if (!handled) {
      // Skip unknown keys
      if (!skip_value(&p, end)) { return -1; }
    }

    p = skip_ws(p, end);
    if (p >= end) { return -1; }

    if (*p == ',') { p++; continue; }
    if (*p == '}') { p++; break; }
    return -1;
  }

  ${predicateEval}
  return 1;
}

// Single-object entry point (backwards compatible).
int prefilter(const char* input, size_t length) {
  return prefilter_object(input, length);
}

// NDJSON batch entry point.
// Scans newline-delimited JSON. For each matching object, writes (offset, length) pair
// into the output buffer. Returns number of matching items, or -1 on error/overflow.
int prefilter_ndjson(const char* input, size_t input_len, uint32_t* output, size_t output_capacity) {
  if (output_capacity < 4) { return -1; } // need at least header + 1 match slot
  size_t max_matches = (output_capacity - 2) / 2;
  int32_t count = 0;
  int32_t total = 0;
  const char* p = input;
  const char* end = input + input_len;

  while (p < end) {
    while (p < end && (*p == '\n' || *p == '\r')) { p++; }
    if (p >= end) { break; }

    const char* line_start = p;
    while (p < end && *p != '\n') { p++; }
    const char* line_end = p;

    if (line_end > line_start && *(line_end - 1) == '\r') {
      line_end--;
    }

    size_t line_len = (size_t)(line_end - line_start);
    if (line_len == 0) { continue; }

    total++;
    int result = prefilter_object(line_start, line_len);

    if (result != 0) {
      if ((size_t)count >= max_matches) { return -1; }
      uint32_t offset = (uint32_t)(line_start - input);
      output[2 + count * 2] = offset;
      output[2 + count * 2 + 1] = (uint32_t)line_len;
      count++;
    }
  }

  output[0] = (uint32_t)total;
  output[1] = 0;
  return count;
}

// JSON array batch entry point.
// Scans a top-level JSON array of objects. For each matching object, writes (offset, length)
// pair into the output buffer. Returns number of matching items, or -1 on error/overflow.
int prefilter_json_array(const char* input, size_t input_len, uint32_t* output, size_t output_capacity) {
  if (output_capacity < 4) { return -1; }
  size_t max_matches = (output_capacity - 2) / 2;
  int32_t count = 0;
  int32_t total = 0;
  const char* p = input;
  const char* end = input + input_len;

  p = skip_ws(p, end);
  if (p >= end || *p != '[') { return -1; }
  p++;

  while (1) {
    p = skip_ws(p, end);
    if (p >= end) { return -1; }
    if (*p == ']') { break; }

    if (*p != '{') { return -1; }

    const char* obj_start = p;
    int depth = 0;
    bool in_string = false;
    bool escape = false;
    const char* scan = p;
    while (scan < end) {
      char c = *scan;
      if (in_string) {
        if (escape) {
          escape = false;
        } else if (c == '\\') {
          escape = true;
        } else if (c == '"') {
          in_string = false;
        }
        scan++;
        continue;
      }
      if (c == '"') {
        in_string = true;
        scan++;
        continue;
      }
      if (c == '{') {
        depth++;
        scan++;
        continue;
      }
      if (c == '}') {
        depth--;
        scan++;
        if (depth == 0) { break; }
        continue;
      }
      scan++;
    }

    if (depth != 0) { return -1; }

    size_t obj_len = (size_t)(scan - obj_start);
    p = scan;
    total++;

    int result = prefilter_object(obj_start, obj_len);

    if (result != 0) {
      if ((size_t)count >= max_matches) { return -1; }
      uint32_t offset = (uint32_t)(obj_start - input);
      output[2 + count * 2] = offset;
      output[2 + count * 2 + 1] = (uint32_t)obj_len;
      count++;
    }

    p = skip_ws(p, end);
    if (p >= end) { return -1; }
    if (*p == ',') { p++; continue; }
    if (*p == ']') { break; }
    return -1;
  }

  output[0] = (uint32_t)total;
  output[1] = 0;
  return count;
}
`;
}

function renderNodeMatching(node: FieldNode, plan: PrefilterPlan, depth: number): string {
  const entries = Array.from(node.children.entries());
  if (entries.length === 0) return "";

  const keyVar = depth === 0 ? "key_start" : `key${depth}_start`;
  const keyLenVar = depth === 0 ? "key_len" : `key${depth}_len`;
  const handledVar = depth === 0 ? "handled" : `handled${depth}`;

  const branches = entries.map(([key, child]) => {
    const keyBytes = encoder.encode(key);
    const keyLiteral = bytesToCString(keyBytes);
    const keyLen = keyBytes.length;

    const hasPredicates = child.predicateIndices.length > 0;
    const hasChildren = child.children.size > 0;

    let body: string;

    if (hasPredicates && !hasChildren) {
      // Pure leaf - read value and check predicates
      body = renderValueReaderByIndices(child.predicateIndices, plan);
    } else if (!hasPredicates && hasChildren) {
      // Pure branch - descend into nested object
      body = renderNestedDescent(child, plan, depth + 1);
    } else if (hasPredicates && hasChildren) {
      // Both leaf and branch - rare but possible
      // If value is '{', descend; otherwise evaluate leaf predicates
      body = `if (*p == '{') {
        ${renderNestedDescentInner(child, plan, depth + 1)}
      } else {
        ${renderValueReaderByIndicesInline(child.predicateIndices, plan)}
      }`;
    } else {
      body = `if (!skip_value(&p, end)) { return -1; }`;
    }

    return `if (${keyLenVar} == ${keyLen} && memcmp(${keyVar}, "${keyLiteral}", ${keyLen}) == 0) {
      ${handledVar} = 1;
      ${body}
    }`;
  }).join(" else ");

  return branches;
}

function renderNestedDescent(node: FieldNode, plan: PrefilterPlan, depth: number): string {
  const inner = renderNestedDescentInner(node, plan, depth);
  return `if (*p == '{') {
        ${inner}
      } else {
        // Expected object but got something else - skip
        if (!skip_value(&p, end)) { return -1; }
      }`;
}

function renderNestedDescentInner(node: FieldNode, plan: PrefilterPlan, depth: number): string {
  const keyVar = `key${depth}_start`;
  const keyLenVar = `key${depth}_len`;
  const keyUnescVar = `key${depth}_needs_unescape`;
  const handledVar = `handled${depth}`;

  const innerMatching = renderNodeMatching(node, plan, depth);

  return `p++; // skip '{'
        while (p < end) {
            p = skip_ws(p, end);
            if (p >= end) { return -1; }
            if (*p == '}') { p++; break; }

            const char* ${keyVar} = NULL;
            size_t ${keyLenVar} = 0;
            bool ${keyUnescVar} = false;
            if (!scan_string(&p, end, &${keyVar}, &${keyLenVar}, &${keyUnescVar})) { return -1; }
            if (${keyUnescVar}) { return -1; }
            p = skip_ws(p, end);
            if (p >= end || *p != ':') { return -1; }
            p++;
            p = skip_ws(p, end);
            if (p >= end) { return -1; }

            int ${handledVar} = 0;
            ${innerMatching}

            if (!${handledVar}) {
                if (!skip_value(&p, end)) { return -1; }
            }

            p = skip_ws(p, end);
            if (p >= end) { return -1; }
            if (*p == ',') { p++; continue; }
            if (*p == '}') { p++; break; }
            return -1;
        }`;
}

function renderValueReaderByIndices(indices: number[], plan: PrefilterPlan): string {
  const refs = indices.map(index => ({ pred: plan.predicates[index]!, index }));
  if (refs.length === 0) return "";
  const stringChecks = refs.map(entry => renderPredicateValueCheck(entry.pred, entry.index, "string")).join("\n");
  const numberChecks = refs.map(entry => renderPredicateValueCheck(entry.pred, entry.index, "number")).join("\n");
  const boolChecks = refs.map(entry => renderPredicateValueCheck(entry.pred, entry.index, "boolean")).join("\n");
  const nullChecks = refs.map(entry => renderPredicateValueCheck(entry.pred, entry.index, "null")).join("\n");
  return `
      if (*p == '"') {
        const char* value_start = NULL; size_t value_len = 0; bool value_needs_unescape = false;
        if (!scan_string(&p, end, &value_start, &value_len, &value_needs_unescape)) { return -1; }
        ${stringChecks}
      } else if (*p == '-' || (*p >= '0' && *p <= '9')) {
        double number = 0;
        if (!parse_number(&p, end, &number)) { return -1; }
        ${numberChecks}
      } else if (p + 4 <= end && memcmp(p, "true", 4) == 0) {
        p += 4;
        int bool_val = 1;
        ${boolChecks}
      } else if (p + 5 <= end && memcmp(p, "false", 5) == 0) {
        p += 5;
        int bool_val = 0;
        ${boolChecks}
      } else if (p + 4 <= end && memcmp(p, "null", 4) == 0) {
        p += 4;
        ${nullChecks}
      } else {
        return -1;
      }
    `;
}

// Same as renderValueReaderByIndices but without wrapping - for inline use in combined branch+leaf nodes
function renderValueReaderByIndicesInline(indices: number[], plan: PrefilterPlan): string {
  return renderValueReaderByIndices(indices, plan);
}

function renderPredicateValueCheck(
  pred: PrefilterPlan["predicates"][number],
  index: number,
  valueType: "string" | "number" | "boolean" | "null"
): string {
  const tag = `p${index}`;
  const seen = `${tag}_seen = 1;`;
  // Mandatory predicates can trigger early-exit on failure.
  // "ne" and "notIn" are soft - missing field means pass, so no early-exit on fail.
  const isMandatory = pred.op !== "ne" && pred.op !== "notIn";
  const earlyExit = isMandatory ? `if (!${tag}_pass) { return 0; }` : "";

  if (pred.op === "isNull") {
    return `
        ${seen}
        ${tag}_pass = ${valueType === "null" ? "1" : "0"};
        ${earlyExit}
        `;
  }
  if (pred.op === "notNull") {
    return `
        ${seen}
        ${tag}_pass = ${valueType === "null" ? "0" : "1"};
        ${earlyExit}
        `;
  }
  if (pred.op === "gt" || pred.op === "gte" || pred.op === "lt" || pred.op === "lte") {
    if (valueType === "number" && pred.type === "number") {
      const value = Number(pred.value ?? 0);
      const op = pred.op === "gt"
        ? ">"
        : pred.op === "gte"
          ? ">="
          : pred.op === "lt"
            ? "<"
            : "<=";
      return `
        ${seen}
        ${tag}_pass = (number ${op} ${value});
        ${earlyExit}
        `;
    }
    if (valueType === "string" && pred.type === "string") {
      const targetStr = String(pred.value ?? "");
      const targetBytes = encoder.encode(targetStr);
      const targetLiteral = bytesToCString(targetBytes);
      const targetLen = targetBytes.length;
      // Lexicographic string comparison for date range checks.
      // Compare using memcmp on the shorter length, then break ties by length.
      const cmpOp = pred.op === "gt"
        ? "> 0"
        : pred.op === "gte"
          ? ">= 0"
          : pred.op === "lt"
            ? "< 0"
            : "<= 0";
      return `
        ${seen}
        {
          size_t min_len = value_len < ${targetLen} ? value_len : ${targetLen};
          int cmp = memcmp(value_start, "${targetLiteral}", min_len);
          if (cmp == 0) { cmp = (int)value_len - (int)${targetLen}; }
          ${tag}_pass = (cmp ${cmpOp});
        }
        ${earlyExit}
        `;
    }
    // Type mismatch - fail this predicate
    return `
      ${seen}
      ${tag}_pass = 0;
      ${earlyExit}
      `;
  }

  if (pred.op === "eq" || pred.op === "ne" || pred.op === "in" || pred.op === "notIn") {
    if (pred.type === "string" && valueType === "string") {
      const values = pred.op === "in" || pred.op === "notIn"
        ? (pred.value as ReadonlyArray<string> | undefined) ?? []
        : [String(pred.value ?? "")];
      const literal = values.map((value) => {
        const bytes = encoder.encode(String(value));
        const encoded = bytesToCString(bytes);
        return { encoded, length: bytes.length };
      });
      const checks = literal
        .map(({ encoded, length }) => `match_string(value_start, value_len, "${encoded}", ${length}, value_needs_unescape) == 1`)
        .join(" || ");
      return `
        ${seen}
        int match = ${checks.length > 0 ? `(${checks})` : "0"};
        ${tag}_pass = ${pred.op === "ne" || pred.op === "notIn" ? "!match" : "match"};
        ${earlyExit}
        `;
    }
    if (pred.type === "number" && valueType === "number") {
      const values = pred.op === "in" || pred.op === "notIn"
        ? (pred.value as ReadonlyArray<number> | undefined) ?? []
        : [Number(pred.value ?? 0)];
      const checks = values.map((value) => `number == ${value}`).join(" || ");
      return `
        ${seen}
        int match = ${checks.length > 0 ? `(${checks})` : "0"};
        ${tag}_pass = ${pred.op === "ne" || pred.op === "notIn" ? "!match" : "match"};
        ${earlyExit}
        `;
    }
    if (pred.type === "boolean" && valueType === "boolean") {
      const values = pred.op === "in" || pred.op === "notIn"
        ? (pred.value as ReadonlyArray<boolean> | undefined) ?? []
        : [Boolean(pred.value)];
      const checks = values.map((value) => `bool_val == ${value ? "1" : "0"}`).join(" || ");
      return `
        ${seen}
        int match = ${checks.length > 0 ? `(${checks})` : "0"};
        ${tag}_pass = ${pred.op === "ne" || pred.op === "notIn" ? "!match" : "match"};
        ${earlyExit}
        `;
    }
    if (pred.type === "null" && valueType === "null") {
      return `
        ${seen}
        ${tag}_pass = ${pred.op === "ne" || pred.op === "notIn" ? "0" : "1"};
        ${earlyExit}
        `;
    }
    return `
        ${seen}
        ${tag}_pass = ${pred.op === "ne" || pred.op === "notIn" ? "1" : "0"};
        ${earlyExit}
        `;
  }
  return "";
}

function bytesToCString(bytes: Uint8Array): string {
  let output = "";
  for (let i = 0; i < bytes.length; i++) {
    const value = bytes[i]!;
    if (value === 0x22) { output += "\\\""; continue; }
    if (value === 0x5C) { output += "\\\\"; continue; }
    if (value >= 0x20 && value <= 0x7E) {
      output += String.fromCharCode(value);
      continue;
    }
    output += `\\x${value.toString(16).padStart(2, "0")}`;
  }
  return output;
}
