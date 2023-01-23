const ApiError = require("./ApiError");

module.exports = (err, req, res, next) => {
  // set locals, only providing error in development
  res.locals.message = err.message;
  res.locals.error = req.app.get("env") === "development" ? err : {};

  if (err instanceof ApiError) {
    res.status(err.code).json(err.message);
  }

  res.status(500).json("Something went wrong :(");
};
