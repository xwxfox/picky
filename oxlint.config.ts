import nkzw from '@nkzw/oxlint-config';
import { defineConfig } from "oxlint";

export default defineConfig({
    extends: [nkzw],
    options: {
        typeAware: true,
    },
    overrides: [
        {
            files: ["**/*.test.ts"],
            rules: {
                "eslint-plugin-unicorn/consistent-function-scoping": "off",
                "perfectionist/sort-objects": "off",
                "eslint/no-console": "off",
            }
        },
        {
            files: ["examples/**/*.ts", "scripts/**/*.ts"],
            rules: {
                "eslint-plugin-unicorn/consistent-function-scoping": "off",
                "eslint/no-console": "off",
            }
        }
    ],
    rules: {
        "@nkzw/no-instanceof": "off",
        "eslint/no-console": "warn",
        "eslint/no-plusplus": "off",
        "perfectionist/sort-object-types": "off",
        "perfectionist/sort-objects": "off",
        "typescript-eslint/no-restricted-types": "off",
        "typescript-eslint/prefer-for-of": "off",
        "typescript/no-empty-object-type": "off",
    }
});
