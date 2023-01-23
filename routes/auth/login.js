const express = require("express");
const router = express.Router();
const { pool_zb } = require("../../config/db");
const ApiError = require("../../error/ApiError");
const crypto = require("crypto");
const issueJWT = require("../../utils/issueJWT");
const isAuth = require("../../middlewares/isAuth");
const matchHexPwd = require("../../utils/matchHexPwd");

/* POST login */
router.post("/", async (req, res, next) => {
  console.log(req.body);
  const { username, password } = req.body;

  if (!username || !password) {
    next(ApiError.badRequest("Invalid username or password!"));
  } else {
    try {
      const pool_zbq = await pool_zb();
      const users = await pool_zbq.query(`
      SELECT name, login_name, login_pwd, email
      FROM [ZebraDB].[dbo].[staff]
      where login_name = '${username}'
      `);

      if (!users.recordset.length) {
        return res.status(401).json("ชื่อผู้ใช้งานไม่ถูกต้อง!");
      }

      /**
       * * hash MD5 and compare with retrieved hash
       */
      const pwdToMD5 = crypto
        .createHash("md5")
        .update(password)
        .digest("hex")
        .toUpperCase();

      //* Check if MD5 hash is exist in array of rows
      //* Loop over recordsets of Array of user rows to match md5 hash string
      const user = matchHexPwd(users.recordset, pwdToMD5); // if match return user row otherwise empty array

      if (!user.length) {
        return res.status(401).json("ชื่อผู้ใช้งาน หรือ รหัสผ่าน ไม่ถูกต้อง!");
      }


      /**
       * * set JWT in cookie for authentication
       */
      const token = await issueJWT.signAccessToken(user[0].login_name);
      return res.status(200).json({
        data: {
          username: user[0].login_name,
          name: user[0].name,
          email: user[0].email,
        },
        token: {
          accessToken: token,
        },
      });
    } catch (err) {
      console.error(err);
      next(err);
    }
  }
});

router.get("/auth", isAuth, async (req, res, next) => {
  /**
   * If token is valid then retrive user data from db
   */
  const pool_zbq = await pool_zb();
  const user = await pool_zbq.query(`
      SELECT *
      FROM [ZebraDB].[dbo].[staff]
      where login_name = '${req.user}'
      `);
  return res.status(200).json({
    username: user.recordset[0].login_name,
    email: user.recordset[0].email,
    name: user.recordset[0].name,
    token: req.token,
  });
});

module.exports = router;
