'use strict';
function Counter(){
  if(!(this instanceof Counter)) return new Counter();
  this.count = 0;
}

Counter.prototype.incr = function(){
  return ++this.count;
}

Counter.prototype.decr = function(){
  return --this.count;
}

module.exports = Counter;
