const issueJWT = require("../utils/issueJWT");
const ApiError = require("../error/ApiError");

module.exports = async (req, res, next) => {
  /**
   * Extract Bearer token from request header
   */
  const bearer = req.header("authorization");

  if (!bearer) {
    return next(ApiError.unauthorized("Invalid Token!"));
  }

  const token = bearer.split(" ")[1];

  if (bearer.split(" ")[1] === null) {
    return next(ApiError.unauthorized("Invalid Token!"));
  }

  try {
    const verifiedToken = await issueJWT.verifyAccessToken(token);
    req.user = verifiedToken.data;
    req.token = token;
    next();
  } catch (err) {
    next(err);
  }
};
