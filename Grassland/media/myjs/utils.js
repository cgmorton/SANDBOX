Array.prototype.max = function() {
  return Math.max.apply(null, this);
};

Array.prototype.min = function() {
  return Math.min.apply(null, this);
};

Array.prototype.median = function()  {
    this.sort( function(a,b) {return a - b;} );

    var half = Math.floor(this.length/2);

    if(this.length % 2)
        return this[half];
    else
        return (this[half-1] + this[half]) / 2.0;
}

function sortNumber(a,b) {
    return a - b;
}

function percentile(lst, p) {
    lst.sort(sortNumber);
    index = p/100. * (lst.length-1);
    if (Math.floor(index) == index) {
        result = lst[index];
    } else {
        i = Math.floor(index)
        fraction = index - i;
        result = lst[i] + (lst[i+1] - lst[i]) * fraction;
    }
    return result;
}

// Returns the percentile of the given value in a sorted numeric array.
function percentRank(arr, v) {
    if (typeof v !== 'number') throw new TypeError('v must be a number');
    for (var i = 0, l = arr.length; i < l; i++) {
        if (v <= arr[i]) {
            while (i < l && v === arr[i]) i++;
            if (i === 0) return 0;
            if (v !== arr[i-1]) {
                i += (v - arr[i-1]) / (arr[i] - arr[i-1]);
            }
            return i / l;
        }
    }
    return 1;
}
