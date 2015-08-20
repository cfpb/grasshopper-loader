module.exports = function(props, fields){
  var vals = new Array(fields.length);

  for(var i=0; i<fields.length; i++){
    var val;
    var field = fields[i];
    if(field.type === 'dynamic'){
      val = new Function('props', field.value)(props); //eslint-disable-line
    }else{
      val = props[field.value];
    }
    vals[i] = val;
  }

  return vals;
}
