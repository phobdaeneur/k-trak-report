const createError = require("http-errors");
const express = require("express");
const path = require("path");
const cookieParser = require("cookie-parser");
const bodyParser = require("body-parser");
const logger = require("morgan");
const handler = require("./error/handler");
const cors = require("cors");

const apiRouter = require("./routes/api/index");
const authRouter = require("./routes/auth/login");

const app = express();

var allowedOrigins = [
  "http://geotracker.kratostracking.com",
  "https://geotracker.kratostracking.com",
  "http://tempreport.kratostracking.com",
  "https://tempreport.kratostracking.com",
  "http://localhost:3000",
];

app.use(
  cors({
    origin: function (origin, callback) {
      // allow requests with no origin
      // (like mobile apps or curl requests)
      if (!origin) return callback(null, true);
      if (allowedOrigins.indexOf(origin) === -1) {
        var msg =
          "The CORS policy for this site does not " +
          "allow access from the specified Origin.";
        return callback(new Error(msg), false);
      }
      return callback(null, true);
    },
  })
);

// Middlewares
app.use(logger("dev"));
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(bodyParser.json());
app.use(express.static(path.join(__dirname, "public")));

app.use("/api", apiRouter);
app.use("/login", authRouter);
app.get("/", (req, res, next) => {
  res.status(200).json({ message: "gotcha!" });
});

// catch 404 and forward to error handler
app.use(function (req, res, next) {
  next(createError(404));
});

// error handler
app.use(handler);

module.exports = app;
