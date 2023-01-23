const { get } = require("./pool-manager");
require("dotenv").config();

const { SQL_USER, SQL_PASSWORD, SQL_DATABASE1, SQL_DATABASE2, SQL_SERVER } =
  process.env;

const zb_config = {
  user: SQL_USER,
  password: SQL_PASSWORD,
  server: SQL_SERVER,
  database: SQL_DATABASE1,
  options: {
    encrypt: false,
  },
};
const zb_log_config = {
  user: SQL_USER,
  password: SQL_PASSWORD,
  server: SQL_SERVER,
  database: SQL_DATABASE2,
  options: {
    encrypt: false,
  },
};

const pool_zb = async () => {
  try {
    const r = await get("pool_zb", zb_config);
    console.log("Successfully connected to Database");
    return r;
  } catch (err) {
    console.log(err);
  }
};
const pool_zb_log = async () => {
  try {
    const r = await get("pool_zb_log", zb_log_config);
    console.log("Successfully connected to Database1");
    return r;
  } catch (err) {
    console.log(err);
  }
};

module.exports = { pool_zb, pool_zb_log };
