var winston = require('winston');

module.exports = function(options){
  var transport;

  if(options.quiet){
    transport = new (winston.transports.Console)({name: 'error', level: 'error'})
  }else{
    transport = new (winston.transports.Console)({name: 'info', level: 'info'})
  }

  var logger = new winston.Logger({
    transports: [transport]
  });

  options.logger = logger;

  return logger;
}
