const fs = require('fs');
const express = require("express");
const Q = require("q");

// Each line has the format ((nasip-addr, dslam), number-of-users)
const lineRegex = /\(\((.+),(.+)\),(.+)\)/;

// Location of files
const sessionsDirectory = "/tmp/streamoutput/sessions/";
const cdrRateDirectory = "/tmp/streamoutput/cdrRate/";
const eventsDirectory = "/tmp/streamoutput/events/";

// Name of this process
process.title="flowview-web";

// Instantiate express
var app=express();

app.use("/", express.static(__dirname));

app.get("/", function(req, res){
    res.json({});
});

app.get("/sessions", function(req, res){
    serveFile(res, sessionsDirectory, pushSessions)
});

app.get("/cdrRate", function(req, res){
    serveFile(res, cdrRateDirectory, pushStats)
});

app.get("/events", function(req, res){
    serveFile(res, eventsDirectory, pushEvents)
});

app.listen(7182, function () {
    console.log('FlowView listening in port 7182');
});

function serveFile(res, dirName, pushFn){
    Q.nfcall(fs.readdir, dirName)
        .then(function(files){
            var response = [];
            var fileIndex = 0;
            addFileToResponse();

            function addFileToResponse(){
                if(fileIndex == files.length){
                    // All the files have been read. Send the response
                    res.json(response);
                } else {
                    fileIndex++;
                    if(files[fileIndex-1].match("^part-[0-9]{5}$")){
                        fs.readFile(dirName+files[fileIndex-1], function(err, data){
                            if(err) console.log("error "+err.message);
                            else {
                                var lines = data.toString().split(("\n"));
                                for(var i = 0; i < lines.length; i++) if(lines[i].trim().length > 0) pushFn(response, lines[i]);
                            }
                            // Continue with next file
                            addFileToResponse();
                        });
                    } else {
                        // Continue with next file
                        addFileToResponse();
                    }
                }
            }
        })
        .catch(function(error){
            res.status(500).send(error.message);
        })
}

// Input format is ((nasip, dslam), (cdrRate, meanCdrRate, stdDevCdrRate, numberOfSigmas))
// Output format is {topElement:{nas:<>, dslam:<>}, cdrRate:<>, meanCdrRate:<>, stdDevCdrRate:<>, numberOfSigmas:<>}
function pushStats(response, line){
	var lineItems = line.replace(/\(/g, "").replace(/\)/g, "").split(",");
	if(lineItems.length == 6){
		response.push(
			{
				topElement: {nas: lineItems[0], dslam: lineItems[1]},
				cdrRate: parseFloat(parseFloat(lineItems[2]).toFixed(5)),
				meanCdrRate: parseFloat(parseFloat(lineItems[3]).toFixed(5)),
				stdDevCdrRate: parseFloat(parseFloat(lineItems[4]).toFixed(5)),
				numberOfSigmas: parseFloat(parseFloat(lineItems[5]).toFixed(5))
			}
		);
	} else console.error("pushStats found element with number of elements != 6 "+line);
}

// Input format is ((nasip, dslam), numberOfSessions)
// Output format is {topElement:{nas:<>, dslam:<>}, nSessions:<>}
function pushSessions(response, line){
	var lineItems = line.replace(/\(/g, "").replace(/\)/g, "").split(",");
	if(lineItems.length == 3){
		response.push(
			{
				topElement: {nas: lineItems[0], dslam: lineItems[1]},
				nSessions: parseInt(lineItems[2])
			}
		);
	} else console.error("pushSessions found element with number of elements != 3 "+line);
}

// Input format is ((nasip, dslam),ArrayBuffer((timestamp,sigma), (timestamp, sigma)))
// Output format is {topElement:{nas:<>, dslam:<>}, timestamp:<>, sigma:<>}
function pushEvents(response, line){
	var lineItems = line.replace(/\(/g, "").replace(/\)/g, "").replace("ArrayBuffer", "").split(",");
	if(lineItems.length < 4 ) console.error("pushEvents found element with number of elements < 4 "+line);
	for(var i = 2; i < lineItems.length; i = i + 2){
		response.push(
			{
				topElement: {nas: lineItems[0], dslam: lineItems[1]},
				timestamp: Number(lineItems[i]),
				sigma: parseFloat(parseFloat(lineItems[i+1]).toFixed(5))
			}
		);
	}
}

