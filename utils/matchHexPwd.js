function matchHexPwd(arr, targetHash) {
    if (!arr.length && !targetHash) {
      return;
    }
  
    var result = [];
  
    for (item of arr) {
      if (targetHash === item.login_pwd.replace(/-/g, "")) {
        result.push(item);
      }
    }
  
    return result;
  }
  
  module.exports = matchHexPwd;
  