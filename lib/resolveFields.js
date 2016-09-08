function joinWithSeparators(parts, separators){
  var val = '';
  if(!separators) separators = {}

  parts.forEach(function(v, i){
    var defaultSep = i===0 ? '' : ' ';
    val += (separators[i] === undefined ? defaultSep : separators[i]) + v;
  })

  if(separators[parts.length] !== undefined) val+=separators[parts.length];

  return val
}

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
      val = joinWithSeparators(field.value.map(mapProps).filter(truthy), field.separators)
    }else{
      val = props[field.value];
    }

    if(val !== null && val !== undefined) val = (val + '').trim();

    vals[keys[i]] = val;
  }

  return vals;
}


function truthy(v){
  return v;
}
