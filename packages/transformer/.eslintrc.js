const itwinjsRecommended = require("@itwin/eslint-plugin/dist/configs/itwinjs-recommended");

module.exports = {
  rules: {
    "@itwin/no-internal": [
      "warn",
      {
        tag: [
          "internal"
        ]
      }
    ],
    "@typescript-eslint/naming-convention": [
      ...itwinjsRecommended.rules["@typescript-eslint/naming-convention"],
      {
        selector: "objectLiteralProperty",
        format: null,
        leadingUnderscore: "allowSingleOrDouble"
      },
    ]
  },
  "parserOptions": {
    "project": "./tsconfig.json"
  }
};

