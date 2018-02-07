function has(s,c) {
  var r = new RegExp("(^| )" + c + "\W*");
  return ( r.test(s) ? true : false );
}

function addEvent( obj, type, fn ) {
  if ( obj.attachEvent ) {
    obj['e'+type+fn] = fn;
    obj[type+fn] = function(){obj['e'+type+fn]( window.event );}
    obj.attachEvent( 'on'+type, obj[type+fn] );
  } else
    obj.addEventListener( type, fn, false );
}

function removeEvent( obj, type, fn ) {
  if ( obj.detachEvent ) {
    obj.detachEvent( 'on'+type, obj[type+fn] );
    obj[type+fn] = null;
  } else
    obj.removeEventListener( type, fn, false );
}

function extractNumber(el) {
  //TODO: handle other locales which have different separators
  return Number.parseFloat(el.innerText.replace(',', ''));
}


addEvent( window, "load", function() {
  var a = document.getElementsByTagName("*") || document.all;

  for ( var i = 0; i < a.length; i++ )
    if ( has( a[i].className, "green_yellow_orange_60_40_25_unbounded" ) ) {
      var percent_val = extractNumber(a[i]);

      if ( percent_val != null && percent_val >= 0) {
        if ( percent_val >= 60 ) percent_categ = "w3-green";
        else if ( percent_val >= 40 ) percent_categ = "w3-light-green";
        else if ( percent_val >= 20 ) percent_categ = "w3-yellow";
        else percent_categ = "w3-orange";
        a[i].className += (" " + percent_categ);
        a[i].style.fontWeight = "bolder";
  //      alert( a[i].className + ": " + percent_categ );
      }
    } else
    if ( has( a[i].className, "traffic_light_90_75_unbounded" ) ) {
      var percent_val = extractNumber(a[i]);

      if ( percent_val != null && percent_val >= 0) {
        if ( percent_val >= 90 ) percent_categ = "w3-green";
        else if ( percent_val >= 75 ) percent_categ = "w3-yellow";
        else percent_categ = "w3-red";
        a[i].className += (" " + percent_categ);
        a[i].style.fontWeight = "bolder";
  //      alert( a[i].className + ": " + percent_categ );
      }
    } else 
    if ( has( a[i].className, "traffic_light_90_80" ) ) {
      var percent_val = extractNumber(a[i]);

      if ( percent_val != null && percent_val >= 0 && percent_val <= 100) {
        if ( percent_val >= 90 ) percent_categ = "w3-green";
        else if ( percent_val >= 80 ) percent_categ = "w3-yellow";
        else percent_categ = "w3-red";
        a[i].className += (" " + percent_categ);
        a[i].style.fontWeight = "bolder";
  //      alert( a[i].className + ": " + percent_categ );
      }
    } else 
    if ( has( a[i].className, "traffic_light_60_50" ) ) {
      var percent_val = extractNumber(a[i]);

      if ( percent_val != null && percent_val >= 0 && percent_val <= 100) {
        if ( percent_val >= 60 ) percent_categ = "w3-green";
        else if ( percent_val >= 50 ) percent_categ = "w3-yellow";
        else percent_categ = "w3-red";
        a[i].className += (" " + percent_categ);
        a[i].style.fontWeight = "bolder";
  //      alert( a[i].className + ": " + percent_categ );
      }
    } else 
    if ( has( a[i].className, "traffic_light_71_unbounded" ) ) {
      var percent_val = extractNumber(a[i]);

      if ( percent_val != null && percent_val >= 0) {
        if ( percent_val >= 71 ) percent_categ = "w3-green";
        else percent_categ = "w3-yellow";
        a[i].className += (" " + percent_categ);
        a[i].style.fontWeight = "bolder";
  //      alert( a[i].className + ": " + percent_categ );
      }
    } else 
    if ( has( a[i].className, "traffic_light_71" ) ) {
      var percent_val = extractNumber(a[i]);

      if ( percent_val != null && percent_val >= 0 && percent_val <= 100) {
        if ( percent_val >= 71 ) percent_categ = "w3-green";
        else percent_categ = "w3-yellow";
        a[i].className += (" " + percent_categ);
        a[i].style.fontWeight = "bolder";
  //      alert( a[i].className + ": " + percent_categ );
      }
    } else 
    if ( has( a[i].className, "unary_good_80_unbounded" ) ) {
      var percent_val = extractNumber(a[i]);

      if ( percent_val != null && percent_val >= 0) {
        if ( percent_val >= 80 ) {
          percent_categ = "w3-green";
          a[i].className += (" " + percent_categ);
        }
        a[i].style.fontWeight = "bolder";
  //      alert( a[i].className + ": " + percent_categ );
      }
    } else 
    if ( has( a[i].className, "unary_good_80" ) ) {
      var percent_val = extractNumber(a[i]);

      if ( percent_val != null && percent_val >= 0 && percent_val <= 100) {
        if ( percent_val >= 80 ) {
          percent_categ = "w3-green";
          a[i].className += (" " + percent_categ);
        }
        a[i].style.fontWeight = "bolder";
  //      alert( a[i].className + ": " + percent_categ );
      }
    } else 
    if ( has( a[i].className, "unary_bad_half_percent" ) ) {
      var percent_val = extractNumber(a[i]);

      if ( percent_val != null && percent_val >= 0 && percent_val <= 100) {
        if ( percent_val > 0.5 ) {
          percent_categ = "w3-red";
          a[i].className += (" " + percent_categ);
        }
        a[i].style.fontWeight = "bolder";
  //      alert( a[i].className + ": " + percent_categ );
      }
    }
} );


addEvent( window, "load", function() {
  var a = document.getElementsByTagName("*") || document.all;

  for ( var i = 0; i < a.length; i++ )
    if ( has( a[i].className, "rise_fall" ) ) {
      var current_val = extractNumber(a[i]);
      if (current_val != null && 'previous' in a[i].attributes) {
        var previous_val = a[i].attributes['previous'].value;
        if (previous_val != null & previous_val != '') {
          var percent_categ = "same";
          if ( current_val > previous_val ) percent_categ = "rise";
          else if ( current_val < previous_val ) percent_categ = "fall";
          if (percent_categ == "rise")
          	a[i].innerHTML = a[i].innerHTML + " &uArr;";
          else if (percent_categ == "fall")
          	a[i].innerHTML = a[i].innerHTML + " &dArr;";
          else
          	a[i].innerHTML = a[i].innerHTML + " &hArr;";
    //      alert( a[i].innerHTML + ": " + percent_categ );
        }
      }
    }
} );