import { defineConfig, type DefineConfigItem } from "bunup";
export default defineConfig({
    name: "picky",
    entry: "src/index.ts",
    dts: {
        inferTypes: true,
        tsgo: true,
    },
    minify: true,
    clean: true,
    format: "esm",
    target: "bun",
    minifySyntax: true,
    minifyWhitespace: true,
    unused: true,
    exports: {
        customExports: () => ({
            "./package.json": "./package.json",
            "./smol": {
                "import": {
                    "types": "./dist/index.d.ts",
                    "default": "./dist/index.js",
                }
            },
            ".": {
                "import": {
                    "types": "./src/index.ts",
                    "default": "./src/index.ts",
                    "bun": "./src/index.ts"
                }
            }
        }),
    },
    sourcemap: false,
    onSuccess: async () => {
        const pkgJson = await Bun.file("./package.json").json() as { version: string, types: string, module: string };
        pkgJson.types = "./src/index.ts";
        pkgJson.module = "./src/index.ts";
        await Bun.write("./package.json", JSON.stringify(pkgJson, null, 2));
    }
}) as DefineConfigItem


