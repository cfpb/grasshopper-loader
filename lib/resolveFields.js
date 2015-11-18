
module.exports = function(props, fields){
  var vals = {};
  var keys = Object.keys(fields);

  if(!fields.Number || !fields.Street || !fields.City || !fields.State || !fields.Zip){
    throw new Error('Invalid fields. Must contain metadata on Number, Street, City, State, and Zip.');
  }

  function mapProps(v){
    return props[v];
  }

  for(var i=0; i<keys.length; i++){
    var val;
    var field = fields[keys[i]];
    if(field.type === 'dynamic'){
      val = new Function('props', field.value)(props); //eslint-disable-line
    }else if(field.type === 'multi'){
      val = field.value.map(mapProps).filter(truthy).join(' ');
    }else{
      val = props[field.value];
    }

    val = (val + '').trim();

    vals[keys[i]] = val;
  }

  return vals;
}


function truthy(v){
  return v;
}
