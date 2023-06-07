import dotenv from "dotenv";

const { error } = dotenv.config();

if (error)
  throw new Error("no env file found");
