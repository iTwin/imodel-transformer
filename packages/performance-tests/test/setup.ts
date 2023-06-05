import assert from "assert";
import dotenv from "dotenv";

const { error } = dotenv.config();

if (error)
  console.error("no env file found");

