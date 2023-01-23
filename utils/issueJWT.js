const jwt = require("jsonwebtoken");
require("dotenv").config();

/**
 * @param {string} payload password string to sign as jwt token
 * @returns {Promise<string>}
 */
const signAccessToken = async (payload) => {
  return new Promise((resolve, reject) => {
    jwt.sign(
      {
        data: payload,
      },
      process.env.SECRET_KEY,
      {
        algorithm: "HS256",
		    expiresIn: "7 days",
      },
      function (err, token) {
        if (err) {
          reject(err);
        }
        resolve(token);
      }
    );
  });
};

/**
 * @param {string} token jwt token string
 */
const verifyAccessToken = async (token) => {
  return new Promise((resolve, reject) => {
    jwt.verify(token, process.env.SECRET_KEY, (err, decoded) => {
      if (err) {
        reject(err);
        // throw err;
      }
      resolve(decoded);
    });
  });
};

// const signRefreshToken = async (str) => {};
// const verifyRefreshToken = async (str) => {};

module.exports = { signAccessToken, verifyAccessToken };
