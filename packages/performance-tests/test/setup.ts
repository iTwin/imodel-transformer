import assert from "assert";
import dotenv from "dotenv";

const { error } = dotenv.config();

if (error)
  console.log("no env file found");

