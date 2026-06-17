const itwinjsRecommended = require("@itwin/eslint-plugin/dist/configs/itwinjs-recommended");
const iTwinPlugin = require("@itwin/eslint-plugin");
const prettier = require("eslint-config-prettier/prettier");

module.exports = [
  {
    files: ["**/*.ts"],
    ...iTwinPlugin.configs.iTwinjsRecommendedConfig,
  },
  {
    files: ["**/*.ts"],
    rules: {
      "@itwin/no-internal": [
        "warn",
        {
          tag: ["internal"],
        },
      ],
      "@typescript-eslint/naming-convention": [
        ...itwinjsRecommended.rules["@typescript-eslint/naming-convention"],
        {
          selector: "objectLiteralProperty",
          format: null,
          leadingUnderscore: "allowSingleOrDouble",
        },
      ],
      "@typescript-eslint/indent": ["off"],
      "@typescript-eslint/dot-notation": [
        "error",
        {
          allowProtectedClassPropertyAccess: true,
          allowPrivateClassPropertyAccess: true,
        },
      ],
      /** The following set of rules were manually turned off by using the output of 'npx eslint-config-prettier ./src/IModelTransformer.ts'
       *  which shows conflicting or unnecessary rules when using eslint + prettier.
       */
      "@typescript-eslint/member-delimiter-style": ["off"],
      "@typescript-eslint/no-extra-semi": ["off"],
      "@typescript-eslint/semi": ["off"],
      "@typescript-eslint/space-before-function-paren": ["off"],
      "@typescript-eslint/type-annotation-spacing": ["off"],
      "eol-last": ["off"],
      "max-statements-per-line": ["off"],
      "new-parens": ["off"],
      "no-multiple-empty-lines": ["off"],
      "no-trailing-spaces": ["off"],
      "nonblock-statement-body-position": ["off"],
      "quote-props": ["off"],
      "arrow-parens": ["off"],
      "brace-style": ["off"],
      "comma-dangle": ["off"],
      /** https://github.com/prettier/eslint-config-prettier#special-rules */
      quotes: [
        "error",
        "double",
        { avoidEscape: true, allowTemplateLiterals: false },
      ],
    },
  },
  {
    files: ["**/*.ts"],
    ...prettier,
  },
  {
    files: ["packages/transformer/**/*.ts"],
    rules: {
      "@typescript-eslint/no-unsafe-enum-comparison": "off",
    },
    languageOptions: {
      parserOptions: {
        project: "packages/transformer/tsconfig.json",
        tsconfigRootDir: __dirname,
      },
      sourceType: "commonjs",
    },
  },
  {
    files: ["packages/transformer/src/test/**/*.ts"],
    rules: {
      "@itwin/no-internal-barrel-imports": "off",
      "@typescript-eslint/no-non-null-assertion": "off",
    },
  },
  {
    files: ["packages/performance-tests/**/*.ts"],
    rules: {
      "@typescript-eslint/naming-convention": [
        ...itwinjsRecommended.rules["@typescript-eslint/naming-convention"],
        {
          selector: ["objectLiteralProperty", "typeProperty"],
          format: null,
          leadingUnderscore: "allowSingleOrDouble",
        },
      ],
    },
    languageOptions: {
      parserOptions: {
        project: "packages/performance-tests/tsconfig.json",
        tsconfigRootDir: __dirname,
      },
      sourceType: "commonjs",
    },
  },
  {
    files: ["packages/test-app/**/*.ts"],
    languageOptions: {
      parserOptions: {
        project: "packages/test-app/tsconfig.json",
        tsconfigRootDir: __dirname,
      },
      sourceType: "commonjs",
    },
  },
];
