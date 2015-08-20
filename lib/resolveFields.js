module.exports = function(props, fields){
  var vals = {};
  var keys = Object.keys(fields);
  for(var i=0; i<keys.length; i++){
    var val;
    var field = fields[keys[i]];
    if(field.type === 'dynamic'){
      val = new Function('props', field.value)(props); //eslint-disable-line
    }else{
      val = props[field.value];
    }
    vals[keys[i]] = val;
  }

  return vals;
}
