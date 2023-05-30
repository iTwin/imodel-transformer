
export class TargetIModelVerificationError extends Error {
  constructor() {
    super([
        "Target for resuming from does not have the expected entity ",
        "from the target that the resume state was made with",
      ].join("\n"));
  }
}