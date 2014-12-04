var exec = require('child_process').exec
var command = '../lsf_bindings/get-lsf-stats --feature '
var fs = require('fs')
var config = require('./config.json')

function execute (command, callback) {
  exec(command, function (error, stdout, stderr) {
    callback(JSON.parse(stdout));
  });
};

//INITIALIZE VARIABLES
function init(){
  var args = process.argv
  if (args.length < 3) {
    console.log('usage: <confix_index>')
    process.kill()
  }
  number_of_fields = Object.keys(config['lsf_fields']).length
  var i = args[2]
  field_index = i
  if (i >= number_of_fields)
    field_index = i - number_of_fields
  lsf_field = config['lsf_fields'][field_index]
  token = config['tokens'][i]
  type = 'scat'

  execute(command + lsf_field, function (field) {
    key = Object.keys(field)[0]
    title = field[key]['title']
    if (field[key]['unit'])
      title += ' (' + field[key]['unit'] + ')'
    if (i < number_of_fields)
      createStream(config, token, title, key)
    else {
      type = 'hist'
      createStream(config, config['tokens'][i], title, key)
    }
  });
}

//ACTUALLY CREATE THE STREAM
function createStream (config, token, chartTitle, lsfFeatureName) {
  var username = config['user'],
    apikey = config['apikey'],
    Plotly = require('plotly')(username, apikey),
    Signal = require('./push.js')


  if (type === 'scat') {
    var data = {
      "name": chartTitle,
      'x': [],   // empty arrays since we will be streaming our data to into these arrays
      'y': [], 'type': 'scatter', 'mode': 'lines+markers', marker: {
        color: "rgba(31, 119, 180, 0.96)"
      }, line: {
        color: "rgba(31, 119, 180, 0.31)"
      }, stream: {
        "token": token, "maxpoints": 5000
      }
    }
    var layout = {
      "filename": lsfFeatureName + '-all',
      //, "fileopt": "overwrite"
      "layout": {
        "title": chartTitle
      }, "world_readable": true
    }
  } else {
    var data = [
      {
        "name": chartTitle,
        "x": [],
        "y": [],
        "type": "bar",
        stream: {
          "token": token,
          "maxpoints": 5000
        }
      }
    ]

    var layout = {
//      "fileopt": 'overwrite',
      "filename": lsfFeatureName + '-topn',
      "layout": {
        "barmode": "group",
        "title": chartTitle + " - Top 15 users"
      }
    }
  }
  Plotly.plot(data, layout, function (err, resp) {
    if (err) return console.log("ERROR", err)
    console.log(resp)
    url = resp['url']
    var plotlystream = Plotly.stream(token, function () {})
    var signalstream = Signal(lsfFeatureName)

    plotlystream.on("error", function (err) {
      signalstream.destroy()
    })
    // Okay - stream to our plot!
    signalstream.pipe(plotlystream)

    //add URLS to json file
    fs.readFile('urls.json', 'utf8', function (err, data) {
      var urlData
      var dataToAppend = {
        "type": type,
        "url": url
      }
      if (err || !data || data.length < 5)
        urlData = {}
      else
        urlData = JSON.parse(data)
      if (typeof urlData[lsf_field] == "undefined")
        urlData[lsf_field] = {}
      urlData[lsf_field][type] = url
      fs.writeFile("urls.json", JSON.stringify(urlData), function (err) {
        if (err)
          console.log(err);
      });
    })

  })
}

init()
