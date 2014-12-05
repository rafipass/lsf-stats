Readable = require('readable-stream').Readable
var util = require('util')
var fs = require('fs')


module.exports = ReadData

function getTop (fields) {
  var tuples = []
  for (var key in fields) tuples.push([key, fields[key]])
  tuples.sort(function (a, b) {
    a = a[1]
    b = b[1]
    return a < b ? -1 : (a > b ? 1 : 0)
  });
  return tuples.splice(tuples.length - 15, tuples.length)
}

function ReadData () {

  opt = {}

  if (!(this instanceof ReadData))
    return new ReadData(opt);


  Readable.call(this, opt)

  var self = this
  self._destroyed = false
  self.iv = null

  var tdelta = 1500
    , hz = opt.hz || 1000 / (20 * tdelta) // seconds
    , amp = opt.amp || 1
    , noiseHz = opt.noiseHz || 4 * hz
    , noiseAmp = opt.noiseAmp || 0.3 * amp
    , trendIV = opt.trendIV || (1000 * 1 / hz) // milliseconds
    , lowtrend = opt.lowtrend || -amp
    , hightrend = opt.hightrend || amp
    , sep = (typeof(opt.sep) === 'string') ? opt.sep : "\n"
    , timeFormatter = opt.timeFormatter || getTimeString

  hz *= 2 * Math.PI // compute once here for later ts computations
  noiseHz *= 2 * Math.PI


  if (self.trendIV < 2 * self.tdelta)
    throw new Error("trendIV must be greater than 2*tdelta")


  startSignal()


  function startSignal () {
    var last = 0
      , trend = 0
      , start = Date.now()

    self.iv = setInterval(function () {
      try {
        fs.readFile('lsfdata.json', 'utf8', function (err, data) {
          if (err)
            return
          var x, y, lsfdata
          if (!data || data.length < 20) {
            return
          }
          try{
            lsfdata = JSON.parse(data)
          }catch(e){return}
          var all_users = lsfdata[lsf_field]['users']['all_users']
          if (all_users == 0 && lsf_field != 'pending_jobs')
           // bad datapoint - send a heartbeat instead of data
            self.push(sep);
            return
          if (type == 'scat') {
            y = all_users
            var now = new Date()
              , ts = now.getTime()
            x = timeFormatter(now)
          } else {
            x = []
            y = []
            top_n_users = getTop(lsfdata[lsf_field]['users'])
            var i;
            for (i = 0; i < top_n_users.length; i++) {
              var x_l = top_n_users[i][0]
              var y_l = top_n_users[i][1]
              x.push(x_l)
              y.push(y_l)
            }
          }
          data = {x: x, y: y}
          if (!opt.objectMode) {
            data = JSON.stringify(data) + sep
            data = new Buffer(data, 'utf8');
          }
          self.push(data);
        });

      } catch (e) {
        //DO NOTHINg
      }
    }, tdelta)
  }

  function generateSignal (ts, trend) {
    var y = amp * Math.sin(hz * ts)
      , noise = randNoiseAmp() * Math.sin(noiseHz * ts)
    return y + noise + trend
  }

}

util.inherits(ReadData, Readable)

ReadData.prototype._read = function () {
  if (this._destroyed) {
    this.push(null)
  }
}

ReadData.prototype.destroy = function () {
  clearInterval(this.iv)
  this._destroyed = true
  this.emit("end")
}

function getTimeString (now) {
  var year = "" + now.getFullYear();
  var month = "" + (now.getMonth() + 1);
  if (month.length == 1) {
    month = "0" + month
  }
  var day = "" + now.getDate();
  if (day.length == 1) {
    day = "0" + day
  }
  var hour = "" + now.getHours();
  if (hour.length == 1) {
    hour = "0" + hour
  }
  var minute = "" + now.getMinutes();
  if (minute.length == 1) {
    minute = "0" + minute
  }
  var second = "" + now.getSeconds();
  if (second.length == 1) {
    second = "0" + second
  }
  var ms = "" + now.getMilliseconds();
  while (ms.length < 3) {
    ms = "0" + ms
  }
  return year + "-" + month + "-" + day + " " + hour + ":" + minute + ":" + second + "." + ms;
}

