class ApiError {
  /**
   * @param {number} code - The http indicate code
   * @param {string} message - The corresponding error message
   */
  constructor(code, message) {
    this.code = code;
    this.message = message;
  }
  static badRequest(message) {
    return new ApiError(400, message);
  }
  static unauthorized(message) {
    return new ApiError(401, message);
  }
}
module.exports = ApiError;
